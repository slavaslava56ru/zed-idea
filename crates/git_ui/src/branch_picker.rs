use anyhow::Context as _;
use editor::{Editor, actions::SelectAll};
use fuzzy::StringMatchCandidate;

use collections::HashSet;
use git;
use git::repository::Branch;
use gpui::http_client::Url;
use gpui::{
    Action, AnyElement, App, Context, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable,
    InteractiveElement, IntoElement, Modifiers, ModifiersChangedEvent, ParentElement, Render,
    SharedString, Styled, Subscription, Task, WeakEntity, Window, actions, rems,
};
use picker::{Picker, PickerDelegate, PickerEditorPosition};
use project::git_store::Repository;
use project::project_settings::ProjectSettings;
use settings::Settings;
use std::sync::Arc;
use time::OffsetDateTime;
use ui::{
    Divider, HighlightedLabel, KeyBinding, ListItem, ListItemSpacing, ListSubHeader, Tooltip,
    prelude::*,
};
use ui_input::ErasedEditor;
use util::ResultExt;
use workspace::notifications::DetachAndPromptErr;
use workspace::{ModalView, Workspace};

use crate::{
    branch_picker, git_panel::show_error_toast, open_create_branch_modal, open_rename_branch_modal,
    project_diff::ProjectDiff, resolve_active_repository,
};

actions!(
    branch_picker,
    [
        /// Deletes the selected git branch or remote.
        DeleteBranch,
        /// Filter the list of remotes
        FilterRemotes
    ]
);

const TITLE_BAR_NEW_BRANCH_NAME: &str = "new-branch";
const MAX_TITLE_BAR_RECENT_BRANCHES: usize = 5;

pub fn checkout_branch(
    workspace: &mut Workspace,
    _: &zed_actions::git::CheckoutBranch,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    open(workspace, &zed_actions::git::Branch, window, cx);
}

pub fn switch(
    workspace: &mut Workspace,
    _: &zed_actions::git::Switch,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    open(workspace, &zed_actions::git::Branch, window, cx);
}

pub fn open(
    workspace: &mut Workspace,
    _: &zed_actions::git::Branch,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    let workspace_handle = workspace.weak_handle();
    let repository = resolve_active_repository(workspace, cx);

    workspace.toggle_modal(window, cx, |window, cx| {
        BranchList::new(
            workspace_handle,
            repository,
            BranchListStyle::Modal,
            rems(34.),
            window,
            cx,
        )
    })
}

pub fn popover(
    workspace: WeakEntity<Workspace>,
    modal_style: bool,
    repository: Option<Entity<Repository>>,
    window: &mut Window,
    cx: &mut App,
) -> Entity<BranchList> {
    let (style, width) = if modal_style {
        (BranchListStyle::Modal, rems(34.))
    } else {
        (BranchListStyle::Popover, rems(20.))
    };

    cx.new(|cx| {
        let list = BranchList::new(workspace, repository, style, width, window, cx);
        list.focus_handle(cx).focus(window, cx);
        list
    })
}

pub fn title_bar_popover(
    workspace: WeakEntity<Workspace>,
    repository: Option<Entity<Repository>>,
    window: &mut Window,
    cx: &mut App,
) -> Entity<BranchList> {
    cx.new(|cx| {
        let list = BranchList::new(
            workspace,
            repository,
            BranchListStyle::TitleBarPopover,
            rems(34.),
            window,
            cx,
        );
        list.focus_handle(cx).focus(window, cx);
        list
    })
}

pub fn create_embedded(
    workspace: WeakEntity<Workspace>,
    repository: Option<Entity<Repository>>,
    width: Rems,
    window: &mut Window,
    cx: &mut Context<BranchList>,
) -> BranchList {
    BranchList::new_embedded(workspace, repository, width, window, cx)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BranchListStyle {
    Modal,
    Popover,
    TitleBarPopover,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchSection {
    Recent,
    Local,
    Remote,
}

pub struct BranchList {
    width: Rems,
    pub picker: Entity<Picker<BranchListDelegate>>,
    picker_focus_handle: FocusHandle,
    _subscription: Option<Subscription>,
    embedded: bool,
}

impl BranchList {
    fn new(
        workspace: WeakEntity<Workspace>,
        repository: Option<Entity<Repository>>,
        style: BranchListStyle,
        width: Rems,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut this = Self::new_inner(workspace, repository, style, width, false, window, cx);
        this._subscription = Some(cx.subscribe(&this.picker, |_, _, _, cx| {
            cx.emit(DismissEvent);
        }));
        this
    }

    fn new_inner(
        workspace: WeakEntity<Workspace>,
        repository: Option<Entity<Repository>>,
        style: BranchListStyle,
        width: Rems,
        embedded: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let all_branches_request = repository
            .clone()
            .map(|repository| repository.update(cx, |repository, _| repository.branches()));

        let default_branch_request = repository.clone().map(|repository| {
            repository.update(cx, |repository, _| repository.default_branch(false))
        });
        let dedupe_tracked_remote_branches = !matches!(style, BranchListStyle::TitleBarPopover);

        cx.spawn_in(window, async move |this, cx| {
            let mut all_branches = all_branches_request
                .context("No active repository")?
                .await??;
            let default_branch = default_branch_request
                .context("No active repository")?
                .await
                .map(Result::ok)
                .ok()
                .flatten()
                .flatten();

            let all_branches = cx
                .background_spawn(async move {
                    if dedupe_tracked_remote_branches {
                        let remote_upstreams: HashSet<_> = all_branches
                            .iter()
                            .filter_map(|branch| {
                                branch
                                    .upstream
                                    .as_ref()
                                    .filter(|upstream| upstream.is_remote())
                                    .map(|upstream| upstream.ref_name.clone())
                            })
                            .collect();

                        all_branches.retain(|branch| !remote_upstreams.contains(&branch.ref_name));
                    }

                    all_branches.sort_by_key(|branch| {
                        (
                            !branch.is_head, // Current branch (is_head=true) comes first
                            branch
                                .most_recent_commit
                                .as_ref()
                                .map(|commit| 0 - commit.commit_timestamp),
                        )
                    });

                    all_branches
                })
                .await;

            let _ = this.update_in(cx, |this, window, cx| {
                this.picker.update(cx, |picker, cx| {
                    picker.delegate.default_branch = default_branch;
                    picker.delegate.all_branches = Some(all_branches);
                    picker.refresh(window, cx);
                })
            });

            anyhow::Ok(())
        })
        .detach_and_log_err(cx);

        let delegate = BranchListDelegate::new(workspace, repository, style, cx);
        let picker = cx.new(|cx| {
            match style {
                BranchListStyle::TitleBarPopover => Picker::list(delegate, window, cx),
                BranchListStyle::Modal | BranchListStyle::Popover => {
                    Picker::uniform_list(delegate, window, cx)
                }
            }
            .show_scrollbar(true)
            .modal(!embedded)
        });
        let picker_focus_handle = picker.focus_handle(cx);

        picker.update(cx, |picker, _| {
            picker.delegate.focus_handle = picker_focus_handle.clone();
        });

        Self {
            picker,
            picker_focus_handle,
            width,
            _subscription: None,
            embedded,
        }
    }

    fn new_embedded(
        workspace: WeakEntity<Workspace>,
        repository: Option<Entity<Repository>>,
        width: Rems,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut this = Self::new_inner(
            workspace,
            repository,
            BranchListStyle::Modal,
            width,
            true,
            window,
            cx,
        );
        this._subscription = Some(cx.subscribe(&this.picker, |_, _, _, cx| {
            cx.emit(DismissEvent);
        }));
        this
    }

    pub fn handle_modifiers_changed(
        &mut self,
        ev: &ModifiersChangedEvent,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.picker
            .update(cx, |picker, _| picker.delegate.modifiers = ev.modifiers)
    }

    pub fn handle_delete(
        &mut self,
        _: &branch_picker::DeleteBranch,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.picker.update(cx, |picker, cx| {
            picker
                .delegate
                .delete_at(picker.delegate.selected_index, window, cx)
        })
    }

    pub fn handle_filter(
        &mut self,
        _: &branch_picker::FilterRemotes,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.picker.update(cx, |picker, cx| {
            picker.delegate.branch_filter = picker.delegate.branch_filter.invert();
            picker.update_matches(picker.query(cx), window, cx);
            picker.refresh_placeholder(window, cx);
            cx.notify();
        });
    }
}
impl ModalView for BranchList {}
impl EventEmitter<DismissEvent> for BranchList {}

impl Focusable for BranchList {
    fn focus_handle(&self, _cx: &App) -> FocusHandle {
        self.picker_focus_handle.clone()
    }
}

impl Render for BranchList {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .key_context("GitBranchSelector")
            .w(self.width)
            .on_modifiers_changed(cx.listener(Self::handle_modifiers_changed))
            .on_action(cx.listener(Self::handle_delete))
            .on_action(cx.listener(Self::handle_filter))
            .child(self.picker.clone())
            .when(!self.embedded, |this| {
                this.on_mouse_down_out({
                    cx.listener(move |this, _, window, cx| {
                        this.picker.update(cx, |this, cx| {
                            this.cancel(&Default::default(), window, cx);
                        })
                    })
                })
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Entry {
    SectionHeader {
        label: SharedString,
    },
    Branch {
        branch: Branch,
        positions: Vec<usize>,
        section: Option<BranchSection>,
    },
    BranchAction {
        entry: BranchActionEntry,
    },
    NewUrl {
        url: String,
    },
    NewBranch {
        name: String,
    },
    NewRemoteName {
        name: String,
        url: SharedString,
    },
}

impl Entry {
    fn as_branch(&self) -> Option<&Branch> {
        match self {
            Entry::Branch { branch, .. } => Some(branch),
            _ => None,
        }
    }

    fn name(&self) -> &str {
        match self {
            Entry::SectionHeader { label } => label.as_ref(),
            Entry::Branch { branch, .. } => branch.name(),
            Entry::BranchAction { entry } => entry.label.as_ref(),
            Entry::NewUrl { url, .. } => url.as_str(),
            Entry::NewBranch { name, .. } => name.as_str(),
            Entry::NewRemoteName { name, .. } => name.as_str(),
        }
    }

    fn is_selectable(&self) -> bool {
        !matches!(self, Entry::SectionHeader { .. })
    }

    #[cfg(test)]
    fn is_new_url(&self) -> bool {
        matches!(self, Self::NewUrl { .. })
    }

    #[cfg(test)]
    fn is_new_branch(&self) -> bool {
        matches!(self, Self::NewBranch { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BranchAction {
    Checkout { branch: Branch },
    NewBranchFrom { branch: Branch },
    ShowDiffWithWorkingTree { branch: Branch },
    UpdateCurrentBranch,
    PushCurrentBranch,
    OpenTrackedBranchActions { branch: Branch },
    CompareWithCurrent { branch: Branch },
    MergeIntoCurrent { branch: Branch },
    ConfirmMergeIntoCurrent { branch: Branch },
    Rename { branch: Branch },
    DeleteLocal { branch: Branch },
    ConfirmDeleteLocal { branch: Branch },
    DeleteWithTrackedBranch { branch: Branch },
    ConfirmDeleteWithTrackedBranch { branch: Branch },
    CancelConfirmation { branch: Branch },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BranchActionEntry {
    action: BranchAction,
    label: SharedString,
    icon: IconName,
    disabled: bool,
    opens_submenu: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum BranchFilter {
    /// Show both local and remote branches.
    All,
    /// Only show remote branches.
    Remote,
}

impl BranchFilter {
    fn invert(&self) -> Self {
        match self {
            BranchFilter::All => BranchFilter::Remote,
            BranchFilter::Remote => BranchFilter::All,
        }
    }
}

pub struct BranchListDelegate {
    workspace: WeakEntity<Workspace>,
    matches: Vec<Entry>,
    all_branches: Option<Vec<Branch>>,
    default_branch: Option<SharedString>,
    repo: Option<Entity<Repository>>,
    style: BranchListStyle,
    selected_index: usize,
    last_query: String,
    modifiers: Modifiers,
    branch_filter: BranchFilter,
    state: PickerState,
    focus_handle: FocusHandle,
}

#[derive(Debug)]
enum PickerState {
    /// When we display list of branches/remotes
    List,
    /// When we display actions for a selected branch in the title bar popover.
    BranchActions { branch: Branch },
    /// When we confirm a destructive action from the title bar popover.
    ConfirmBranchAction { action: BranchAction },
    /// When we set an url to create a new remote
    NewRemote,
    /// When we confirm the new remote url (after NewRemote)
    CreateRemote(SharedString),
    /// When we set a new branch to create
    NewBranch,
}

impl BranchListDelegate {
    fn new(
        workspace: WeakEntity<Workspace>,
        repo: Option<Entity<Repository>>,
        style: BranchListStyle,
        cx: &mut Context<BranchList>,
    ) -> Self {
        Self {
            workspace,
            matches: vec![],
            repo,
            style,
            all_branches: None,
            default_branch: None,
            selected_index: 0,
            last_query: Default::default(),
            modifiers: Default::default(),
            branch_filter: BranchFilter::All,
            state: PickerState::List,
            focus_handle: cx.focus_handle(),
        }
    }

    fn current_branch(&self, cx: &App) -> Option<Branch> {
        self.repo
            .as_ref()
            .and_then(|repo| repo.read(cx).branch.clone())
            .or_else(|| {
                self.all_branches
                    .as_ref()?
                    .iter()
                    .find(|branch| branch.is_head)
                    .cloned()
            })
    }

    fn current_branch_name(&self, cx: &App) -> Option<String> {
        self.current_branch(cx)
            .map(|branch| branch.name().to_string())
    }

    fn find_branch_by_ref_name(&self, ref_name: &str) -> Option<Branch> {
        self.all_branches
            .as_ref()?
            .iter()
            .find(|branch| branch.ref_name.as_ref() == ref_name)
            .cloned()
    }

    fn title_bar_grouped_matches(&self, matches: Vec<Entry>) -> Vec<Entry> {
        let mut local_branches = Vec::new();
        let mut remote_branches = Vec::new();

        for entry in matches {
            let Entry::Branch {
                branch, positions, ..
            } = entry
            else {
                continue;
            };

            if branch.is_remote() {
                remote_branches.push((branch, positions));
            } else {
                local_branches.push((branch, positions));
            }
        }

        let mut grouped_matches = Vec::new();
        if local_branches.len() > 1 {
            grouped_matches.push(Entry::SectionHeader {
                label: "Recent".into(),
            });
            grouped_matches.extend(
                local_branches
                    .iter()
                    .take(MAX_TITLE_BAR_RECENT_BRANCHES)
                    .cloned()
                    .map(|(branch, positions)| Entry::Branch {
                        branch,
                        positions,
                        section: Some(BranchSection::Recent),
                    }),
            );
        }

        if !local_branches.is_empty() {
            grouped_matches.push(Entry::SectionHeader {
                label: "Local".into(),
            });
            grouped_matches.extend(local_branches.into_iter().map(|(branch, positions)| {
                Entry::Branch {
                    branch,
                    positions,
                    section: Some(BranchSection::Local),
                }
            }));
        }

        if !remote_branches.is_empty() {
            grouped_matches.push(Entry::SectionHeader {
                label: "Remote".into(),
            });
            grouped_matches.extend(remote_branches.into_iter().map(|(branch, positions)| {
                Entry::Branch {
                    branch,
                    positions,
                    section: Some(BranchSection::Remote),
                }
            }));
        }

        grouped_matches
    }

    fn preferred_selectable_index(&self, preferred_index: usize) -> usize {
        if self.matches.is_empty() {
            return 0;
        }

        let clamped_index = preferred_index.min(self.matches.len().saturating_sub(1));
        if self.matches[clamped_index].is_selectable() {
            return clamped_index;
        }

        self.matches
            .iter()
            .enumerate()
            .skip(clamped_index)
            .find(|(_, entry)| entry.is_selectable())
            .or_else(|| {
                self.matches
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, entry)| entry.is_selectable())
            })
            .map(|(index, _)| index)
            .unwrap_or(0)
    }

    fn branch_action_entries(&self, branch: &Branch, cx: &App) -> Vec<Entry> {
        let current_branch_name = self.current_branch_name(cx);
        let tracked_branch = branch
            .upstream
            .as_ref()
            .and_then(|upstream| self.find_branch_by_ref_name(upstream.ref_name.as_ref()));
        let tracked_branch_name = branch
            .upstream
            .as_ref()
            .and_then(|upstream| upstream.stripped_ref_name());
        let mut entries = Vec::new();

        if !branch.is_head && !branch.is_remote() {
            entries.push(BranchActionEntry {
                action: BranchAction::Checkout {
                    branch: branch.clone(),
                },
                label: "Checkout".into(),
                icon: IconName::GitBranch,
                disabled: false,
                opens_submenu: false,
            });
        }

        entries.push(BranchActionEntry {
            action: BranchAction::NewBranchFrom {
                branch: branch.clone(),
            },
            label: format!("New Branch from '{}'...", branch.name()).into(),
            icon: IconName::GitBranchPlus,
            disabled: false,
            opens_submenu: false,
        });

        if branch.is_head {
            entries.push(BranchActionEntry {
                action: BranchAction::ShowDiffWithWorkingTree {
                    branch: branch.clone(),
                },
                label: "Show Diff with Working Tree".into(),
                icon: IconName::Diff,
                disabled: false,
                opens_submenu: false,
            });

            entries.push(BranchActionEntry {
                action: BranchAction::UpdateCurrentBranch,
                label: "Update".into(),
                icon: IconName::ArrowCircle,
                disabled: false,
                opens_submenu: false,
            });

            entries.push(BranchActionEntry {
                action: BranchAction::PushCurrentBranch,
                label: "Push...".into(),
                icon: IconName::ExpandUp,
                disabled: false,
                opens_submenu: false,
            });
        } else if current_branch_name.is_some() {
            entries.push(BranchActionEntry {
                action: BranchAction::CompareWithCurrent {
                    branch: branch.clone(),
                },
                label: "Compare with current".into(),
                icon: IconName::Diff,
                disabled: false,
                opens_submenu: false,
            });

            entries.push(BranchActionEntry {
                action: BranchAction::MergeIntoCurrent {
                    branch: branch.clone(),
                },
                label: format!(
                    "Merge '{}' into '{}'",
                    branch.name(),
                    current_branch_name.as_deref().unwrap_or("current branch")
                )
                .into(),
                icon: IconName::ArrowCircle,
                disabled: false,
                opens_submenu: false,
            });
        }

        if let Some(tracked_branch_name) = tracked_branch_name {
            if let Some(tracked_branch) = tracked_branch {
                entries.push(BranchActionEntry {
                    action: BranchAction::OpenTrackedBranchActions {
                        branch: tracked_branch,
                    },
                    label: format!("Tracked Branch '{}'", tracked_branch_name).into(),
                    icon: IconName::GitBranchAlt,
                    disabled: false,
                    opens_submenu: true,
                });
            }
        }

        if !branch.is_remote() {
            entries.push(BranchActionEntry {
                action: BranchAction::Rename {
                    branch: branch.clone(),
                },
                label: "Rename...".into(),
                icon: IconName::Pencil,
                disabled: false,
                opens_submenu: false,
            });
        }

        if !branch.is_remote() && !branch.is_head {
            entries.push(BranchActionEntry {
                action: BranchAction::DeleteLocal {
                    branch: branch.clone(),
                },
                label: "Delete local".into(),
                icon: IconName::Trash,
                disabled: false,
                opens_submenu: false,
            });

            if tracked_branch_name.is_some() {
                entries.push(BranchActionEntry {
                    action: BranchAction::DeleteWithTrackedBranch {
                        branch: branch.clone(),
                    },
                    label: "Delete with tracked branch".into(),
                    icon: IconName::Trash,
                    disabled: false,
                    opens_submenu: false,
                });
            }
        }

        entries
            .into_iter()
            .map(|entry| Entry::BranchAction { entry })
            .collect()
    }

    fn branch_action_confirmation_entries(&self, action: &BranchAction, cx: &App) -> Vec<Entry> {
        let current_branch_name = self.current_branch_name(cx);
        let (branch, confirm_action, confirm_label, confirm_icon) = match action {
            BranchAction::MergeIntoCurrent { branch } => (
                branch.clone(),
                BranchAction::ConfirmMergeIntoCurrent {
                    branch: branch.clone(),
                },
                format!(
                    "Confirm merge {} into {}",
                    branch.name(),
                    current_branch_name.as_deref().unwrap_or("current branch")
                ),
                IconName::Check,
            ),
            BranchAction::DeleteLocal { branch } => (
                branch.clone(),
                BranchAction::ConfirmDeleteLocal {
                    branch: branch.clone(),
                },
                format!("Confirm delete {}", branch.name()),
                IconName::Trash,
            ),
            BranchAction::DeleteWithTrackedBranch { branch } => {
                let tracked_branch_name = branch
                    .upstream
                    .as_ref()
                    .and_then(|upstream| upstream.stripped_ref_name())
                    .unwrap_or("tracked branch");

                (
                    branch.clone(),
                    BranchAction::ConfirmDeleteWithTrackedBranch {
                        branch: branch.clone(),
                    },
                    format!(
                        "Confirm delete {} and {}",
                        branch.name(),
                        tracked_branch_name
                    ),
                    IconName::Trash,
                )
            }
            _ => return Vec::new(),
        };

        [
            BranchActionEntry {
                action: confirm_action,
                label: confirm_label.into(),
                icon: confirm_icon,
                disabled: false,
                opens_submenu: false,
            },
            BranchActionEntry {
                action: BranchAction::CancelConfirmation { branch },
                label: "Cancel".into(),
                icon: IconName::Close,
                disabled: false,
                opens_submenu: false,
            },
        ]
        .into_iter()
        .map(|entry| Entry::BranchAction { entry })
        .collect()
    }

    fn branch_action_confirmation_placeholder(&self, action: &BranchAction, cx: &App) -> Arc<str> {
        let current_branch_name = self.current_branch_name(cx);

        match action {
            BranchAction::MergeIntoCurrent { branch } => Arc::<str>::from(format!(
                "Merge {} into {}?",
                branch.name(),
                current_branch_name.as_deref().unwrap_or("current branch")
            )),
            BranchAction::DeleteLocal { branch } => {
                Arc::<str>::from(format!("Delete {}?", branch.name()))
            }
            BranchAction::DeleteWithTrackedBranch { branch } => {
                let tracked_branch_name = branch
                    .upstream
                    .as_ref()
                    .and_then(|upstream| upstream.stripped_ref_name())
                    .unwrap_or("tracked branch");

                Arc::<str>::from(format!(
                    "Delete {} and {}?",
                    branch.name(),
                    tracked_branch_name
                ))
            }
            _ => Arc::<str>::from("Confirm action"),
        }
    }

    fn open_branch_actions(
        &mut self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        self.state = PickerState::BranchActions {
            branch: branch.clone(),
        };
        self.matches = self.branch_action_entries(&branch, cx);
        self.selected_index = 0;
        self.last_query.clear();

        cx.defer_in(window, |picker, window, cx| {
            picker.refresh_placeholder(window, cx);
            picker.set_query("", window, cx);
            cx.notify();
        });
    }

    fn open_branch_action_confirmation(
        &mut self,
        action: BranchAction,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        if !matches!(
            action,
            BranchAction::MergeIntoCurrent { .. }
                | BranchAction::DeleteLocal { .. }
                | BranchAction::DeleteWithTrackedBranch { .. }
        ) {
            return;
        }

        self.state = PickerState::ConfirmBranchAction {
            action: action.clone(),
        };
        self.matches = self.branch_action_confirmation_entries(&action, cx);
        self.selected_index = 0;
        self.last_query.clear();

        cx.defer_in(window, |picker, window, cx| {
            picker.refresh_placeholder(window, cx);
            picker.set_query("", window, cx);
            cx.notify();
        });
    }

    fn checkout_branch_entry(
        &self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(repo) = self.repo.clone() else {
            return;
        };

        cx.spawn(async move |_, cx| {
            repo.update(cx, |repo, _| repo.change_branch(branch.name().to_string()))
                .await??;
            Ok(())
        })
        .detach_and_prompt_err("Failed to change branch", window, cx, |_, _, _| None);
    }

    fn show_create_branch_modal_from(
        &self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(workspace) = self.workspace.upgrade() else {
            return;
        };
        let Some(repo) = self.repo.clone() else {
            return;
        };
        window
            .spawn(cx, async move |cx| {
                workspace
                    .update_in(cx, |workspace, window, cx| {
                        open_create_branch_modal(
                            workspace,
                            branch.name().to_string(),
                            repo.clone(),
                            window,
                            cx,
                        );
                    })
                    .log_err();
                Ok::<(), anyhow::Error>(())
            })
            .detach_and_log_err(cx);
    }

    fn show_rename_branch_modal(
        &self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(workspace) = self.workspace.upgrade() else {
            return;
        };
        let Some(repo) = self.repo.clone() else {
            return;
        };
        window
            .spawn(cx, async move |cx| {
                workspace
                    .update_in(cx, |workspace, window, cx| {
                        open_rename_branch_modal(
                            workspace,
                            branch.name().to_string(),
                            repo.clone(),
                            window,
                            cx,
                        );
                    })
                    .log_err();
                Ok::<(), anyhow::Error>(())
            })
            .detach_and_log_err(cx);
    }

    fn compare_branch_with_current(
        &self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(workspace) = self.workspace.upgrade() else {
            return;
        };
        let base_ref: SharedString = branch.name().to_string().into();
        let repo = self.repo.clone();
        window
            .spawn(cx, async move |cx| {
                workspace
                    .update_in(cx, |workspace, window, cx| {
                        ProjectDiff::open_branch_diff_for_ref(
                            workspace,
                            repo.clone(),
                            base_ref.clone(),
                            window,
                            cx,
                        );
                    })
                    .log_err();
                Ok::<(), anyhow::Error>(())
            })
            .detach_and_log_err(cx);
    }

    fn show_diff_with_working_tree(
        &self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        self.compare_branch_with_current(branch, window, cx);
    }

    fn merge_branch_into_current(
        &self,
        branch: Branch,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(repo) = self.repo.clone() else {
            return;
        };

        cx.spawn(async move |_, cx| {
            repo.update(cx, |repo, _| repo.merge_branch(branch.name().to_string()))
                .await??;
            Ok(())
        })
        .detach_and_prompt_err("Failed to merge branch", window, cx, |_, _, _| None);
    }

    fn delete_branch_entry(
        &self,
        branch: Branch,
        delete_tracked_branch: bool,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(repo) = self.repo.clone() else {
            return;
        };
        let local_branch_name = branch.name().to_string();
        let tracked_branch_name = delete_tracked_branch.then(|| {
            branch
                .upstream
                .as_ref()
                .and_then(|upstream| upstream.stripped_ref_name())
                .map(ToOwned::to_owned)
        });

        cx.spawn(async move |_, cx| {
            repo.update(cx, |repo, _| repo.delete_branch(false, local_branch_name))
                .await??;

            if let Some(tracked_branch_name) = tracked_branch_name.flatten() {
                repo.update(cx, |repo, _| repo.delete_branch(true, tracked_branch_name))
                    .await??;
            }

            Ok(())
        })
        .detach_and_prompt_err("Failed to delete branch", window, cx, |e, _, _| {
            Some(e.to_string())
        });
    }

    fn create_branch(
        &self,
        from_branch: Option<SharedString>,
        new_branch_name: SharedString,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(repo) = self.repo.clone() else {
            return;
        };
        let new_branch_name = new_branch_name.to_string().replace(' ', "-");
        let base_branch = from_branch.map(|b| b.to_string());
        cx.spawn(async move |_, cx| {
            repo.update(cx, |repo, _| {
                repo.create_branch(new_branch_name, base_branch)
            })
            .await??;

            Ok(())
        })
        .detach_and_prompt_err("Failed to create branch", window, cx, |e, _, _| {
            Some(e.to_string())
        });
        cx.emit(DismissEvent);
    }

    fn create_remote(
        &self,
        remote_name: String,
        remote_url: String,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) {
        let Some(repo) = self.repo.clone() else {
            return;
        };

        let receiver = repo.update(cx, |repo, _| repo.create_remote(remote_name, remote_url));

        cx.background_spawn(async move { receiver.await? })
            .detach_and_prompt_err("Failed to create remote", window, cx, |e, _, _cx| {
                Some(e.to_string())
            });
        cx.emit(DismissEvent);
    }

    fn delete_at(&self, idx: usize, window: &mut Window, cx: &mut Context<Picker<Self>>) {
        let Some(entry) = self.matches.get(idx).cloned() else {
            return;
        };
        let Some(repo) = self.repo.clone() else {
            return;
        };

        let workspace = self.workspace.clone();

        cx.spawn_in(window, async move |picker, cx| {
            let is_remote;
            let result = match &entry {
                Entry::Branch { branch, .. } => {
                    if branch.is_head {
                        return Ok(());
                    }

                    is_remote = branch.is_remote();
                    repo.update(cx, |repo, _| {
                        repo.delete_branch(is_remote, branch.name().to_string())
                    })
                    .await?
                }
                _ => {
                    log::error!("Failed to delete entry: wrong entry to delete");
                    return Ok(());
                }
            };

            if let Err(e) = result {
                if is_remote {
                    log::error!("Failed to delete remote branch: {}", e);
                } else {
                    log::error!("Failed to delete branch: {}", e);
                }

                if let Some(workspace) = workspace.upgrade() {
                    cx.update(|_window, cx| {
                        if is_remote {
                            show_error_toast(
                                workspace,
                                format!("branch -dr {}", entry.name()),
                                e,
                                cx,
                            )
                        } else {
                            show_error_toast(
                                workspace,
                                format!("branch -d {}", entry.name()),
                                e,
                                cx,
                            )
                        }
                    })?;
                }

                return Ok(());
            }

            picker.update_in(cx, |picker, _, cx| {
                picker.delegate.matches.retain(|e| e != &entry);

                if let Entry::Branch { branch, .. } = &entry {
                    if let Some(all_branches) = &mut picker.delegate.all_branches {
                        all_branches.retain(|e| e.ref_name != branch.ref_name);
                    }
                }

                if picker.delegate.matches.is_empty() {
                    picker.delegate.selected_index = 0;
                } else if picker.delegate.selected_index >= picker.delegate.matches.len() {
                    picker.delegate.selected_index = picker.delegate.matches.len() - 1;
                }

                cx.notify();
            })?;

            anyhow::Ok(())
        })
        .detach();
    }
}

impl PickerDelegate for BranchListDelegate {
    type ListItem = AnyElement;

    fn placeholder_text(&self, _window: &mut Window, cx: &mut App) -> Arc<str> {
        if matches!(self.style, BranchListStyle::TitleBarPopover) {
            return match &self.state {
                PickerState::List | PickerState::NewRemote | PickerState::NewBranch => {
                    Arc::<str>::from("Search for branches and actions")
                }
                PickerState::BranchActions { branch } => {
                    Arc::<str>::from(format!("Actions for {}", branch.name()))
                }
                PickerState::ConfirmBranchAction { action, .. } => {
                    self.branch_action_confirmation_placeholder(action, cx)
                }
                PickerState::CreateRemote(_) => Arc::<str>::from("Enter a name for this remote…"),
            };
        }

        match &self.state {
            PickerState::List | PickerState::NewRemote | PickerState::NewBranch => {
                match self.branch_filter {
                    BranchFilter::All | BranchFilter::Remote => "Select branch…".to_string(),
                }
            }
            PickerState::BranchActions { branch } => format!("Actions for {}", branch.name()),
            PickerState::ConfirmBranchAction { action, .. } => self
                .branch_action_confirmation_placeholder(action, cx)
                .to_string(),
            PickerState::CreateRemote(_) => "Enter a name for this remote…".to_string(),
        }
        .into()
    }

    fn no_matches_text(&self, _window: &mut Window, _cx: &mut App) -> Option<SharedString> {
        match self.state {
            PickerState::CreateRemote(_) => {
                Some(SharedString::new_static("Remote name can't be empty"))
            }
            _ => None,
        }
    }

    fn can_select(&self, ix: usize, _window: &mut Window, _cx: &mut Context<Picker<Self>>) -> bool {
        self.matches.get(ix).is_some_and(|entry| {
            entry.is_selectable()
                && !matches!(entry, Entry::BranchAction { entry } if entry.disabled)
        })
    }

    fn separators_after_indices(&self) -> Vec<usize> {
        if !matches!(self.style, BranchListStyle::TitleBarPopover) {
            return Vec::new();
        }

        match &self.state {
            PickerState::BranchActions { branch } if branch.is_head && self.matches.len() > 1 => {
                vec![0]
            }
            _ => Vec::new(),
        }
    }

    fn render_editor(
        &self,
        editor: &Arc<dyn ErasedEditor>,
        _window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> Div {
        let editor = editor.as_any().downcast_ref::<Entity<Editor>>().unwrap();

        if matches!(self.style, BranchListStyle::TitleBarPopover) {
            let update_focus_handle = self.focus_handle.clone();
            let commit_focus_handle = self.focus_handle.clone();
            let push_focus_handle = self.focus_handle.clone();
            let new_branch_editor = editor.clone();

            return v_flex()
                .child(
                    h_flex()
                        .overflow_hidden()
                        .flex_none()
                        .h_9()
                        .px_2p5()
                        .child(editor.clone()),
                )
                .child(Divider::horizontal())
                .when(matches!(self.state, PickerState::List), |this| {
                    this.child(
                        v_flex()
                            .px_1()
                            .py_1()
                            .gap_0p5()
                            .child(
                                ListItem::new("git-titlebar-update")
                                    .inset(true)
                                    .spacing(ListItemSpacing::Sparse)
                                    .start_slot(
                                        Icon::new(IconName::ArrowCircle)
                                            .color(Color::Muted)
                                            .size(IconSize::Small),
                                    )
                                    .end_slot(
                                        KeyBinding::for_action_in(
                                            &git::Pull,
                                            &self.focus_handle,
                                            cx,
                                        )
                                        .size(rems_from_px(12.)),
                                    )
                                    .tooltip(move |_, cx| {
                                        Tooltip::for_action_in(
                                            "Update Project",
                                            &git::Pull,
                                            &update_focus_handle,
                                            cx,
                                        )
                                    })
                                    .on_click(cx.listener(|_, _, window, cx| {
                                        window.dispatch_action(Box::new(git::Pull), cx);
                                        cx.emit(DismissEvent);
                                    }))
                                    .child(Label::new("Update")),
                            )
                            .child(
                                ListItem::new("git-titlebar-commit")
                                    .inset(true)
                                    .spacing(ListItemSpacing::Sparse)
                                    .start_slot(
                                        Icon::new(IconName::GitCommit)
                                            .color(Color::Muted)
                                            .size(IconSize::Small),
                                    )
                                    .end_slot(
                                        KeyBinding::for_action_in(
                                            &git::Commit,
                                            &self.focus_handle,
                                            cx,
                                        )
                                        .size(rems_from_px(12.)),
                                    )
                                    .tooltip(move |_, cx| {
                                        Tooltip::for_action_in(
                                            "Commit",
                                            &git::Commit,
                                            &commit_focus_handle,
                                            cx,
                                        )
                                    })
                                    .on_click(cx.listener(|_, _, window, cx| {
                                        window.dispatch_action(Box::new(git::Commit), cx);
                                        cx.emit(DismissEvent);
                                    }))
                                    .child(Label::new("Commit...")),
                            )
                            .child(
                                ListItem::new("git-titlebar-push")
                                    .inset(true)
                                    .spacing(ListItemSpacing::Sparse)
                                    .start_slot(
                                        Icon::new(IconName::ExpandUp)
                                            .color(Color::Muted)
                                            .size(IconSize::Small),
                                    )
                                    .end_slot(
                                        KeyBinding::for_action_in(
                                            &git::Push,
                                            &self.focus_handle,
                                            cx,
                                        )
                                        .size(rems_from_px(12.)),
                                    )
                                    .tooltip(move |_, cx| {
                                        Tooltip::for_action_in(
                                            "Push",
                                            &git::Push,
                                            &push_focus_handle,
                                            cx,
                                        )
                                    })
                                    .on_click(cx.listener(|_, _, window, cx| {
                                        window.dispatch_action(Box::new(git::Push), cx);
                                        cx.emit(DismissEvent);
                                    }))
                                    .child(Label::new("Push...")),
                            )
                            .child(
                                ListItem::new("git-titlebar-create-branch")
                                    .inset(true)
                                    .spacing(ListItemSpacing::Sparse)
                                    .start_slot(
                                        Icon::new(IconName::GitBranchPlus)
                                            .color(Color::Muted)
                                            .size(IconSize::Small),
                                    )
                                    .tooltip(Tooltip::text("Start creating a new branch"))
                                    .on_click(cx.listener(move |this, _, window, cx| {
                                        this.delegate.selected_index = usize::MAX;
                                        this.set_query(TITLE_BAR_NEW_BRANCH_NAME, window, cx);
                                        new_branch_editor.update(cx, |editor, cx| {
                                            editor.select_all(&SelectAll, window, cx);
                                        });
                                    }))
                                    .child(Label::new("New Branch...")),
                            ),
                    )
                    .child(Divider::horizontal())
                });
        }

        let focus_handle = self.focus_handle.clone();

        v_flex()
            .when(
                self.editor_position() == PickerEditorPosition::End,
                |this| this.child(Divider::horizontal()),
            )
            .child(
                h_flex()
                    .overflow_hidden()
                    .flex_none()
                    .h_9()
                    .px_2p5()
                    .child(editor.clone())
                    .when(
                        self.editor_position() == PickerEditorPosition::End,
                        |this| {
                            let tooltip_label = match self.branch_filter {
                                BranchFilter::All => "Filter Remote Branches",
                                BranchFilter::Remote => "Show All Branches",
                            };

                            this.gap_1().justify_between().child({
                                IconButton::new("filter-remotes", IconName::Filter)
                                    .toggle_state(self.branch_filter == BranchFilter::Remote)
                                    .tooltip(move |_, cx| {
                                        Tooltip::for_action_in(
                                            tooltip_label,
                                            &branch_picker::FilterRemotes,
                                            &focus_handle,
                                            cx,
                                        )
                                    })
                                    .on_click(|_click, window, cx| {
                                        window.dispatch_action(
                                            branch_picker::FilterRemotes.boxed_clone(),
                                            cx,
                                        );
                                    })
                            })
                        },
                    ),
            )
            .when(
                self.editor_position() == PickerEditorPosition::Start,
                |this| this.child(Divider::horizontal()),
            )
    }

    fn editor_position(&self) -> PickerEditorPosition {
        match self.style {
            BranchListStyle::Modal => PickerEditorPosition::Start,
            BranchListStyle::Popover => PickerEditorPosition::End,
            BranchListStyle::TitleBarPopover => PickerEditorPosition::Start,
        }
    }

    fn match_count(&self) -> usize {
        self.matches.len()
    }

    fn selected_index(&self) -> usize {
        self.selected_index
    }

    fn set_selected_index(
        &mut self,
        ix: usize,
        _window: &mut Window,
        _: &mut Context<Picker<Self>>,
    ) {
        self.selected_index = ix;
    }

    fn update_matches(
        &mut self,
        query: String,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> Task<()> {
        if let PickerState::ConfirmBranchAction { action, .. } = &self.state {
            if query.is_empty() {
                self.matches = self.branch_action_confirmation_entries(action, cx);
                self.selected_index = 0;
                self.last_query.clear();
                return Task::ready(());
            }

            self.state = PickerState::List;
        }

        if let PickerState::BranchActions { branch } = &self.state {
            if query.is_empty() {
                self.matches = self.branch_action_entries(branch, cx);
                self.selected_index = 0;
                self.last_query.clear();
                return Task::ready(());
            }

            self.state = PickerState::List;
        }

        let Some(all_branches) = self.all_branches.clone() else {
            return Task::ready(());
        };

        let branch_filter = self.branch_filter;
        cx.spawn_in(window, async move |picker, cx| {
            let branch_matches_filter = |branch: &Branch| match branch_filter {
                BranchFilter::All => true,
                BranchFilter::Remote => branch.is_remote(),
            };

                let mut matches: Vec<Entry> = if query.is_empty() {
                    let mut matches: Vec<Entry> = all_branches
                        .into_iter()
                        .filter(|branch| branch_matches_filter(branch))
                        .map(|branch| Entry::Branch {
                            branch,
                            positions: Vec::new(),
                            section: None,
                        })
                        .collect();

                // Keep the existing recency sort within each group, but show local branches first.
                matches.sort_by_key(|entry| entry.as_branch().is_some_and(|b| b.is_remote()));

                matches
            } else {
                let branches = all_branches
                    .iter()
                    .filter(|branch| branch_matches_filter(branch))
                    .collect::<Vec<_>>();
                let candidates = branches
                    .iter()
                    .enumerate()
                    .map(|(ix, branch)| StringMatchCandidate::new(ix, branch.name()))
                    .collect::<Vec<StringMatchCandidate>>();
                let mut matches: Vec<Entry> = fuzzy::match_strings(
                    &candidates,
                    &query,
                    true,
                    true,
                    10000,
                    &Default::default(),
                    cx.background_executor().clone(),
                )
                .await
                .into_iter()
                .map(|candidate| Entry::Branch {
                    branch: branches[candidate.candidate_id].clone(),
                    positions: candidate.positions,
                    section: None,
                })
                .collect();

                // Keep fuzzy-relevance ordering within local/remote groups, but show locals first.
                matches.sort_by_key(|entry| entry.as_branch().is_some_and(|b| b.is_remote()));

                matches
            };
            picker
                .update(cx, |picker, _| {
                    if let PickerState::CreateRemote(url) = &picker.delegate.state {
                        let query = query.replace(' ', "-");
                        if !query.is_empty() {
                            picker.delegate.matches = vec![Entry::NewRemoteName {
                                name: query.clone(),
                                url: url.clone(),
                            }];
                            picker.delegate.selected_index = 0;
                        } else {
                            picker.delegate.matches = Vec::new();
                            picker.delegate.selected_index = 0;
                        }
                        picker.delegate.last_query = query;
                        return;
                    }

                    if !query.is_empty()
                        && !matches.first().is_some_and(|entry| entry.name() == query)
                    {
                        let query = query.replace(' ', "-");
                        let is_url = query.trim_start_matches("git@").parse::<Url>().is_ok();
                        let entry = if is_url {
                            Entry::NewUrl { url: query }
                        } else {
                            Entry::NewBranch { name: query }
                        };
                        // Only transition to NewBranch/NewRemote states when we only show their list item
                        // Otherwise, stay in List state so footer buttons remain visible
                        picker.delegate.state = if matches.is_empty() {
                            if is_url {
                                PickerState::NewRemote
                            } else {
                                PickerState::NewBranch
                            }
                        } else {
                            PickerState::List
                        };
                        matches.push(entry);
                    } else {
                        picker.delegate.state = PickerState::List;
                    }
                    let delegate = &mut picker.delegate;
                    if query.is_empty()
                        && matches!(delegate.style, BranchListStyle::TitleBarPopover)
                        && matches!(delegate.state, PickerState::List)
                    {
                        matches = delegate.title_bar_grouped_matches(matches);
                    }
                    delegate.matches = matches;
                    if delegate.matches.is_empty() {
                        delegate.selected_index = 0;
                    } else {
                        delegate.selected_index =
                            delegate.preferred_selectable_index(delegate.selected_index);
                    }
                    delegate.last_query = query;
                })
                .log_err();
        })
    }

    fn confirm(&mut self, secondary: bool, window: &mut Window, cx: &mut Context<Picker<Self>>) {
        let Some(entry) = self.matches.get(self.selected_index()) else {
            return;
        };

        match entry {
            Entry::SectionHeader { .. } => return,
            Entry::Branch { branch, .. } => {
                let branch = branch.clone();
                if matches!(self.style, BranchListStyle::TitleBarPopover) {
                    self.open_branch_actions(branch, window, cx);
                    return;
                }

                self.checkout_branch_entry(branch, window, cx);
            }
            Entry::BranchAction { entry } => match &entry.action {
                BranchAction::Checkout { branch } => {
                    self.checkout_branch_entry(branch.clone(), window, cx);
                }
                BranchAction::NewBranchFrom { branch } => {
                    self.show_create_branch_modal_from(branch.clone(), window, cx);
                }
                BranchAction::ShowDiffWithWorkingTree { branch } => {
                    self.show_diff_with_working_tree(branch.clone(), window, cx);
                }
                BranchAction::UpdateCurrentBranch => {
                    window.dispatch_action(Box::new(git::Pull), cx);
                }
                BranchAction::PushCurrentBranch => {
                    window.dispatch_action(Box::new(git::Push), cx);
                }
                BranchAction::OpenTrackedBranchActions { branch } => {
                    self.open_branch_actions(branch.clone(), window, cx);
                    return;
                }
                BranchAction::CompareWithCurrent { branch } => {
                    self.compare_branch_with_current(branch.clone(), window, cx);
                }
                BranchAction::MergeIntoCurrent { branch } => {
                    self.open_branch_action_confirmation(
                        BranchAction::MergeIntoCurrent {
                            branch: branch.clone(),
                        },
                        window,
                        cx,
                    );
                    return;
                }
                BranchAction::ConfirmMergeIntoCurrent { branch } => {
                    self.merge_branch_into_current(branch.clone(), window, cx);
                }
                BranchAction::Rename { branch } => {
                    self.show_rename_branch_modal(branch.clone(), window, cx);
                }
                BranchAction::DeleteLocal { branch } => {
                    self.open_branch_action_confirmation(
                        BranchAction::DeleteLocal {
                            branch: branch.clone(),
                        },
                        window,
                        cx,
                    );
                    return;
                }
                BranchAction::ConfirmDeleteLocal { branch } => {
                    self.delete_branch_entry(branch.clone(), false, window, cx);
                }
                BranchAction::DeleteWithTrackedBranch { branch } => {
                    self.open_branch_action_confirmation(
                        BranchAction::DeleteWithTrackedBranch {
                            branch: branch.clone(),
                        },
                        window,
                        cx,
                    );
                    return;
                }
                BranchAction::ConfirmDeleteWithTrackedBranch { branch } => {
                    self.delete_branch_entry(branch.clone(), true, window, cx);
                }
                BranchAction::CancelConfirmation { branch } => {
                    self.open_branch_actions(branch.clone(), window, cx);
                    return;
                }
            },
            Entry::NewUrl { url } => {
                self.state = PickerState::CreateRemote(url.clone().into());
                self.matches = Vec::new();
                self.selected_index = 0;

                cx.defer_in(window, |picker, window, cx| {
                    picker.refresh_placeholder(window, cx);
                    picker.set_query("", window, cx);
                    cx.notify();
                });

                // returning early to prevent dismissing the modal, so a user can enter
                // a remote name first.
                return;
            }
            Entry::NewRemoteName { name, url } => {
                self.create_remote(name.clone(), url.to_string(), window, cx);
            }
            Entry::NewBranch { name } => {
                let from_branch = if secondary {
                    self.default_branch.clone()
                } else {
                    None
                };
                self.create_branch(from_branch, name.into(), window, cx);
            }
        }

        cx.emit(DismissEvent);
    }

    fn dismissed(&mut self, _: &mut Window, cx: &mut Context<Picker<Self>>) {
        self.state = PickerState::List;
        cx.emit(DismissEvent);
    }

    fn render_match(
        &self,
        ix: usize,
        selected: bool,
        _window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> Option<Self::ListItem> {
        let entry = &self.matches.get(ix)?;

        if let Entry::SectionHeader { label } = entry {
            return Some(
                v_flex()
                    .w_full()
                    .when(ix > 0, |this| this.mt_1())
                    .child(
                        ListSubHeader::new(label.clone())
                            .left_icon(Some(IconName::ChevronDown))
                            .inset(true),
                    )
                    .into_any_element(),
            );
        }

        let (commit_time, author_name, subject) =
            if matches!(self.style, BranchListStyle::TitleBarPopover) {
                (None, None, None)
            } else {
                entry
                    .as_branch()
                    .and_then(|branch| {
                        branch.most_recent_commit.as_ref().map(|commit| {
                            let subject = commit.subject.clone();
                            let commit_time =
                                OffsetDateTime::from_unix_timestamp(commit.commit_timestamp)
                                    .unwrap_or_else(|_| OffsetDateTime::now_utc());
                            let local_offset = time::UtcOffset::current_local_offset()
                                .unwrap_or(time::UtcOffset::UTC);
                            let formatted_time = time_format::format_localized_timestamp(
                                commit_time,
                                OffsetDateTime::now_utc(),
                                local_offset,
                                time_format::TimestampFormat::Relative,
                            );
                            let author = commit.author_name.clone();
                            (Some(formatted_time), Some(author), Some(subject))
                        })
                    })
                    .unwrap_or_else(|| (None, None, None))
            };

        let entry_icon = match entry {
            Entry::SectionHeader { .. } => IconName::ChevronDown,
            Entry::BranchAction { entry } => entry.icon,
            Entry::NewUrl { .. } | Entry::NewBranch { .. } | Entry::NewRemoteName { .. } => {
                IconName::Plus
            }
            Entry::Branch {
                branch, section, ..
            } => {
                if matches!(self.style, BranchListStyle::TitleBarPopover) {
                    match section.unwrap_or(BranchSection::Local) {
                        BranchSection::Recent => {
                            if branch.is_head {
                                IconName::StarFilled
                            } else {
                                IconName::Star
                            }
                        }
                        BranchSection::Local => {
                            if branch.is_head {
                                IconName::GitBranch
                            } else {
                                IconName::GitBranchAlt
                            }
                        }
                        BranchSection::Remote => IconName::Screen,
                    }
                } else if branch.is_remote() {
                    IconName::Screen
                } else {
                    IconName::GitBranchAlt
                }
            }
        };

        let entry_title = match entry {
            Entry::SectionHeader { .. } => unreachable!("handled above"),
            Entry::BranchAction { entry } => Label::new(entry.label.clone())
                .single_line()
                .truncate()
                .into_any_element(),
            Entry::NewUrl { .. } => Label::new("Create Remote Repository")
                .single_line()
                .truncate()
                .into_any_element(),
            Entry::NewBranch { name } => Label::new(format!("Create Branch: \"{name}\"…"))
                .single_line()
                .truncate()
                .into_any_element(),
            Entry::NewRemoteName { name, .. } => Label::new(format!("Create Remote: \"{name}\""))
                .single_line()
                .truncate()
                .into_any_element(),
            Entry::Branch {
                branch, positions, ..
            } => {
                HighlightedLabel::new(branch.name().to_string(), positions.clone())
                    .single_line()
                    .truncate()
                    .into_any_element()
            }
        };

        let focus_handle = self.focus_handle.clone();
        let is_new_items = matches!(
            entry,
            Entry::NewUrl { .. } | Entry::NewBranch { .. } | Entry::NewRemoteName { .. }
        );
        let is_disabled = matches!(entry, Entry::BranchAction { entry } if entry.disabled);
        let is_title_bar_popover = matches!(self.style, BranchListStyle::TitleBarPopover);

        let is_head_branch = entry.as_branch().is_some_and(|branch| branch.is_head);
        let upstream_label = if is_title_bar_popover {
            entry
                .as_branch()
                .and_then(|branch| branch.upstream.as_ref())
                .and_then(|upstream| upstream.stripped_ref_name())
                .map(ToOwned::to_owned)
        } else {
            None
        };
        let has_upstream_label = upstream_label.is_some();
        let shows_submenu = matches!(entry, Entry::Branch { .. })
            || matches!(entry, Entry::BranchAction { entry } if entry.opens_submenu);

        let deleted_branch_icon = |entry_ix: usize| {
            IconButton::new(("delete", entry_ix), IconName::Trash)
                .icon_size(IconSize::Small)
                .tooltip(move |_, cx| {
                    Tooltip::for_action_in(
                        "Delete Branch",
                        &branch_picker::DeleteBranch,
                        &focus_handle,
                        cx,
                    )
                })
                .on_click(cx.listener(move |this, _, window, cx| {
                    this.delegate.delete_at(entry_ix, window, cx);
                }))
        };

        let create_from_default_button = self.default_branch.as_ref().map(|default_branch| {
            let tooltip_label: SharedString = format!("Create New From: {default_branch}").into();
            let focus_handle = self.focus_handle.clone();

            IconButton::new("create_from_default", IconName::GitBranchPlus)
                .icon_size(IconSize::Small)
                .tooltip(move |_, cx| {
                    Tooltip::for_action_in(
                        tooltip_label.clone(),
                        &menu::SecondaryConfirm,
                        &focus_handle,
                        cx,
                    )
                })
                .on_click(cx.listener(|this, _, window, cx| {
                    this.delegate.confirm(true, window, cx);
                }))
                .into_any_element()
        });

        Some(
            ListItem::new(format!("vcs-menu-{ix}"))
                .inset(true)
                .spacing(ListItemSpacing::Sparse)
                .toggle_state(selected)
                .disabled(is_disabled)
                .child(
                    h_flex()
                        .w_full()
                        .gap_2p5()
                        .flex_grow()
                        .child(
                            Icon::new(entry_icon)
                                .color(Color::Muted)
                                .size(IconSize::Small),
                        )
                        .child(
                            v_flex()
                                .id("info_container")
                                .w_full()
                                .child(entry_title)
                                .when(!is_title_bar_popover, |this| {
                                    this.child({
                                        let message = match entry {
                                            Entry::SectionHeader { .. } => String::new(),
                                            Entry::BranchAction { .. } => String::new(),
                                            Entry::NewUrl { url } => format!("Based off {url}"),
                                            Entry::NewRemoteName { url, .. } => {
                                                format!("Based off {url}")
                                            }
                                            Entry::NewBranch { .. } => {
                                                if let Some(current_branch) =
                                                    self.repo.as_ref().and_then(|repo| {
                                                        repo.read(cx)
                                                            .branch
                                                            .as_ref()
                                                            .map(|b| b.name())
                                                    })
                                                {
                                                    format!("Based off {}", current_branch)
                                                } else {
                                                    "Based off the current branch".to_string()
                                                }
                                            }
                                            Entry::Branch { .. } => String::new(),
                                        };

                                        if matches!(entry, Entry::Branch { .. }) {
                                            let show_author_name = ProjectSettings::get_global(cx)
                                                .git
                                                .branch_picker
                                                .show_author_name;
                                            let has_author =
                                                show_author_name && author_name.is_some();
                                            let has_commit = commit_time.is_some();
                                            let author_for_meta =
                                                if show_author_name { author_name } else { None };

                                            let dot = || {
                                                Label::new("•")
                                                    .alpha(0.5)
                                                    .color(Color::Muted)
                                                    .size(LabelSize::Small)
                                            };

                                            h_flex()
                                                .w_full()
                                                .min_w_0()
                                                .gap_1p5()
                                                .when_some(author_for_meta, |this, author| {
                                                    this.child(
                                                        Label::new(author)
                                                            .color(Color::Muted)
                                                            .size(LabelSize::Small),
                                                    )
                                                })
                                                .when_some(commit_time, |this, time| {
                                                    this.when(has_author, |this| this.child(dot()))
                                                        .child(
                                                            Label::new(time)
                                                                .color(Color::Muted)
                                                                .size(LabelSize::Small),
                                                        )
                                                })
                                                .when_some(subject, |this, subj| {
                                                    this.when(has_commit, |this| this.child(dot()))
                                                        .child(
                                                            Label::new(subj.to_string())
                                                                .color(Color::Muted)
                                                                .size(LabelSize::Small)
                                                                .truncate()
                                                                .flex_1(),
                                                        )
                                                })
                                                .when(!has_commit, |this| {
                                                    this.child(
                                                        Label::new("No commits found")
                                                            .color(Color::Muted)
                                                            .size(LabelSize::Small),
                                                    )
                                                })
                                                .into_any_element()
                                        } else {
                                            Label::new(message)
                                                .size(LabelSize::Small)
                                                .color(Color::Muted)
                                                .truncate()
                                                .into_any_element()
                                        }
                                    })
                                })
                                .when_some(
                                    entry.as_branch().map(|b| b.name().to_string()),
                                    |this, branch_name| {
                                        this.map(|this| {
                                            if is_head_branch {
                                                this.tooltip(move |_, cx| {
                                                    Tooltip::with_meta(
                                                        branch_name.clone(),
                                                        None,
                                                        "Current Branch",
                                                        cx,
                                                    )
                                                })
                                            } else {
                                                this.tooltip(Tooltip::text(branch_name))
                                            }
                                        })
                                    },
                                ),
                        ),
                )
                .when_some(upstream_label, |this, upstream| {
                    this.end_slot(
                        h_flex()
                            .gap_1p5()
                            .items_center()
                            .child(
                                Label::new(upstream)
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .when(shows_submenu, |this| {
                                this.child(
                                    Icon::new(IconName::ChevronRight)
                                        .size(IconSize::XSmall)
                                        .color(Color::Muted),
                                )
                            }),
                    )
                })
                .when(
                    is_title_bar_popover && !has_upstream_label && shows_submenu,
                    |this| {
                        this.end_slot(
                            Icon::new(IconName::ChevronRight)
                                .size(IconSize::XSmall)
                                .color(Color::Muted),
                        )
                    },
                )
                .when(
                    !is_title_bar_popover && !is_new_items && !is_head_branch,
                    |this| {
                        this.end_slot(deleted_branch_icon(ix))
                            .show_end_slot_on_hover()
                    },
                )
                .when_some(
                    if is_new_items {
                        create_from_default_button
                    } else {
                        None
                    },
                    |this, create_from_default_button| {
                        this.end_slot(create_from_default_button)
                            .show_end_slot_on_hover()
                    },
                )
                .into_any_element(),
        )
    }

    fn render_footer(&self, _: &mut Window, cx: &mut Context<Picker<Self>>) -> Option<AnyElement> {
        if self.editor_position() == PickerEditorPosition::End
            || matches!(self.style, BranchListStyle::TitleBarPopover)
        {
            return None;
        }
        let focus_handle = self.focus_handle.clone();

        let footer_container = || {
            h_flex()
                .w_full()
                .p_1p5()
                .border_t_1()
                .border_color(cx.theme().colors().border_variant)
        };

        match self.state {
            PickerState::List => {
                let selected_entry = self.matches.get(self.selected_index);

                let branch_from_default_button = self
                    .default_branch
                    .as_ref()
                    .filter(|_| matches!(selected_entry, Some(Entry::NewBranch { .. })))
                    .map(|default_branch| {
                        let button_label = format!("Create New From: {default_branch}");

                        Button::new("branch-from-default", button_label)
                            .key_binding(
                                KeyBinding::for_action_in(
                                    &menu::SecondaryConfirm,
                                    &focus_handle,
                                    cx,
                                )
                                .map(|kb| kb.size(rems_from_px(12.))),
                            )
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.delegate.confirm(true, window, cx);
                            }))
                    });

                let delete_and_select_btns = h_flex()
                    .gap_1()
                    .when(
                        !selected_entry
                            .and_then(|entry| entry.as_branch())
                            .is_some_and(|branch| branch.is_head),
                        |this| {
                            this.child(
                                Button::new("delete-branch", "Delete")
                                    .key_binding(
                                        KeyBinding::for_action_in(
                                            &branch_picker::DeleteBranch,
                                            &focus_handle,
                                            cx,
                                        )
                                        .map(|kb| kb.size(rems_from_px(12.))),
                                    )
                                    .on_click(|_, window, cx| {
                                        window.dispatch_action(
                                            branch_picker::DeleteBranch.boxed_clone(),
                                            cx,
                                        );
                                    }),
                            )
                        },
                    )
                    .child(
                        Button::new("select_branch", "Select")
                            .key_binding(
                                KeyBinding::for_action_in(&menu::Confirm, &focus_handle, cx)
                                    .map(|kb| kb.size(rems_from_px(12.))),
                            )
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.delegate.confirm(false, window, cx);
                            })),
                    );

                Some(
                    footer_container()
                        .map(|this| {
                            if branch_from_default_button.is_some() {
                                this.justify_end().when_some(
                                    branch_from_default_button,
                                    |this, button| {
                                        this.child(button).child(
                                            Button::new("create", "Create")
                                                .key_binding(
                                                    KeyBinding::for_action_in(
                                                        &menu::Confirm,
                                                        &focus_handle,
                                                        cx,
                                                    )
                                                    .map(|kb| kb.size(rems_from_px(12.))),
                                                )
                                                .on_click(cx.listener(|this, _, window, cx| {
                                                    this.delegate.confirm(false, window, cx);
                                                })),
                                        )
                                    },
                                )
                            } else {
                                this.justify_between()
                                    .child({
                                        let focus_handle = focus_handle.clone();
                                        let filter_label = match self.branch_filter {
                                            BranchFilter::All => "Filter Remote",
                                            BranchFilter::Remote => "Show All",
                                        };
                                        Button::new("filter-remotes", filter_label)
                                            .toggle_state(matches!(
                                                self.branch_filter,
                                                BranchFilter::Remote
                                            ))
                                            .key_binding(
                                                KeyBinding::for_action_in(
                                                    &branch_picker::FilterRemotes,
                                                    &focus_handle,
                                                    cx,
                                                )
                                                .map(|kb| kb.size(rems_from_px(12.))),
                                            )
                                            .on_click(|_click, window, cx| {
                                                window.dispatch_action(
                                                    branch_picker::FilterRemotes.boxed_clone(),
                                                    cx,
                                                );
                                            })
                                    })
                                    .child(delete_and_select_btns)
                            }
                        })
                        .into_any_element(),
                )
            }
            PickerState::BranchActions { .. } | PickerState::ConfirmBranchAction { .. } => None,
            PickerState::NewBranch => {
                let branch_from_default_button =
                    self.default_branch.as_ref().map(|default_branch| {
                        let button_label = format!("Create New From: {default_branch}");

                        Button::new("branch-from-default", button_label)
                            .key_binding(
                                KeyBinding::for_action_in(
                                    &menu::SecondaryConfirm,
                                    &focus_handle,
                                    cx,
                                )
                                .map(|kb| kb.size(rems_from_px(12.))),
                            )
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.delegate.confirm(true, window, cx);
                            }))
                    });

                Some(
                    footer_container()
                        .gap_1()
                        .justify_end()
                        .when_some(branch_from_default_button, |this, button| {
                            this.child(button)
                        })
                        .child(
                            Button::new("branch-from-default", "Create")
                                .key_binding(
                                    KeyBinding::for_action_in(&menu::Confirm, &focus_handle, cx)
                                        .map(|kb| kb.size(rems_from_px(12.))),
                                )
                                .on_click(cx.listener(|this, _, window, cx| {
                                    this.delegate.confirm(false, window, cx);
                                })),
                        )
                        .into_any_element(),
                )
            }
            PickerState::CreateRemote(_) => Some(
                footer_container()
                    .justify_end()
                    .child(
                        Button::new("branch-from-default", "Confirm")
                            .key_binding(
                                KeyBinding::for_action_in(&menu::Confirm, &focus_handle, cx)
                                    .map(|kb| kb.size(rems_from_px(12.))),
                            )
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.delegate.confirm(false, window, cx);
                            }))
                            .disabled(self.last_query.is_empty()),
                    )
                    .into_any_element(),
            ),
            PickerState::NewRemote => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use git::repository::{CommitSummary, Remote, Upstream, UpstreamTrackingStatus};
    use gpui::{AppContext, TestAppContext, VisualTestContext};
    use project::{FakeFs, Project};
    use rand::{Rng, rngs::StdRng};
    use serde_json::json;
    use settings::SettingsStore;
    use util::path;
    use workspace::MultiWorkspace;

    fn init_test(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
            editor::init(cx);
        });
    }

    fn create_test_branch(
        name: &str,
        is_head: bool,
        remote_name: Option<&str>,
        timestamp: Option<i64>,
    ) -> Branch {
        let ref_name = match remote_name {
            Some(remote_name) => format!("refs/remotes/{remote_name}/{name}"),
            None => format!("refs/heads/{name}"),
        };

        Branch {
            is_head,
            ref_name: ref_name.into(),
            upstream: None,
            most_recent_commit: timestamp.map(|ts| CommitSummary {
                sha: "abc123".into(),
                commit_timestamp: ts,
                author_name: "Test Author".into(),
                subject: "Test commit".into(),
                has_parent: true,
            }),
        }
    }

    fn create_test_branches() -> Vec<Branch> {
        vec![
            create_test_branch("main", true, None, Some(1000)),
            create_test_branch("feature-auth", false, None, Some(900)),
            create_test_branch("feature-ui", false, None, Some(800)),
            create_test_branch("develop", false, None, Some(700)),
        ]
    }

    fn create_test_branch_with_upstream(
        name: &str,
        is_head: bool,
        remote_name: &str,
        timestamp: Option<i64>,
    ) -> Branch {
        let mut branch = create_test_branch(name, is_head, None, timestamp);
        branch.upstream = Some(Upstream {
            ref_name: format!("refs/remotes/{remote_name}/{name}").into(),
            tracking: UpstreamTrackingStatus {
                ahead: 0,
                behind: 0,
            }
            .into(),
        });
        branch
    }

    async fn init_branch_list_test(
        style: BranchListStyle,
        repository: Option<Entity<Repository>>,
        branches: Vec<Branch>,
        cx: &mut TestAppContext,
    ) -> (Entity<BranchList>, VisualTestContext) {
        let fs = FakeFs::new(cx.executor());
        let project = Project::test(fs, [], cx).await;

        let window_handle =
            cx.add_window(|window, cx| MultiWorkspace::test_new(project, window, cx));
        let workspace = window_handle
            .read_with(cx, |mw, _| mw.workspace().clone())
            .unwrap();

        let branch_list = window_handle
            .update(cx, |_multi_workspace, window, cx| {
                cx.new(|cx| {
                    let mut delegate =
                        BranchListDelegate::new(workspace.downgrade(), repository, style, cx);
                    delegate.all_branches = Some(branches);
                    let picker = cx.new(|cx| match style {
                        BranchListStyle::TitleBarPopover => Picker::list(delegate, window, cx),
                        BranchListStyle::Modal | BranchListStyle::Popover => {
                            Picker::uniform_list(delegate, window, cx)
                        }
                    });
                    let picker_focus_handle = picker.focus_handle(cx);
                    picker.update(cx, |picker, _| {
                        picker.delegate.focus_handle = picker_focus_handle.clone();
                    });

                    let _subscription = cx.subscribe(&picker, |_, _, _, cx| {
                        cx.emit(DismissEvent);
                    });

                    BranchList {
                        picker,
                        picker_focus_handle,
                        width: rems(34.),
                        _subscription: Some(_subscription),
                        embedded: false,
                    }
                })
            })
            .unwrap();

        let cx = VisualTestContext::from_window(window_handle.into(), cx);

        (branch_list, cx)
    }

    async fn init_fake_repository(
        cx: &mut TestAppContext,
    ) -> (Entity<Project>, Entity<Repository>) {
        let fs = FakeFs::new(cx.executor());
        fs.insert_tree(
            path!("/dir"),
            json!({
                ".git": {},
                "file.txt": "buffer_text".to_string()
            }),
        )
        .await;
        fs.set_head_for_repo(
            path!("/dir/.git").as_ref(),
            &[("file.txt", "test".to_string())],
            "deadbeef",
        );
        fs.set_index_for_repo(
            path!("/dir/.git").as_ref(),
            &[("file.txt", "index_text".to_string())],
        );

        let project = Project::test(fs.clone(), [path!("/dir").as_ref()], cx).await;
        let repository = cx.read(|cx| project.read(cx).active_repository(cx));

        (project, repository.unwrap())
    }

    #[gpui::test]
    async fn test_update_branch_matches_with_query(cx: &mut TestAppContext) {
        init_test(cx);

        let branches = create_test_branches();
        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, None, branches, cx).await;
        let cx = &mut ctx;

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                let query = "feature".to_string();
                branch_list.picker.update(cx, |picker, cx| {
                    picker.delegate.update_matches(query, window, cx)
                })
            })
            .await;
        cx.run_until_parked();

        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                // Should have 2 existing branches + 1 "create new branch" entry = 3 total
                assert_eq!(picker.delegate.matches.len(), 3);
                assert!(
                    picker
                        .delegate
                        .matches
                        .iter()
                        .any(|m| m.name() == "feature-auth")
                );
                assert!(
                    picker
                        .delegate
                        .matches
                        .iter()
                        .any(|m| m.name() == "feature-ui")
                );
                // Verify the last entry is the "create new branch" option
                let last_match = picker.delegate.matches.last().unwrap();
                assert!(last_match.is_new_branch());
            })
        });
    }

    async fn update_branch_list_matches_with_empty_query(
        branch_list: &Entity<BranchList>,
        cx: &mut VisualTestContext,
    ) {
        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker.delegate.update_matches(String::new(), window, cx)
                })
            })
            .await;
        cx.run_until_parked();
    }

    fn branch_action_labels(matches: &[Entry]) -> Vec<(String, bool)> {
        matches
            .iter()
            .map(|entry| match entry {
                Entry::BranchAction { entry } => (entry.label.to_string(), entry.disabled),
                _ => panic!("expected branch action entry"),
            })
            .collect()
    }

    fn branch_match_index(matches: &[Entry], branch_name: &str) -> usize {
        matches
            .iter()
            .position(|entry| matches!(entry, Entry::Branch { branch, .. } if branch.name() == branch_name))
            .expect("branch should be present")
    }

    #[gpui::test]
    async fn test_delete_branch(cx: &mut TestAppContext) {
        init_test(cx);
        let (_project, repository) = init_fake_repository(cx).await;

        let branches = create_test_branches();

        let branch_names = branches
            .iter()
            .map(|branch| branch.name().to_string())
            .collect::<Vec<String>>();
        let repo = repository.clone();
        cx.spawn(async move |mut cx| {
            for branch in branch_names {
                repo.update(&mut cx, |repo, _| repo.create_branch(branch, None))
                    .await
                    .unwrap()
                    .unwrap();
            }
        })
        .await;
        cx.run_until_parked();

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, repository.into(), branches, cx).await;
        let cx = &mut ctx;

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        let branch_to_delete = branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                assert_eq!(picker.delegate.matches.len(), 4);
                let branch_to_delete = picker.delegate.matches.get(1).unwrap().name().to_string();
                picker.delegate.delete_at(1, window, cx);
                branch_to_delete
            })
        });
        cx.run_until_parked();

        let expected_branches = ["main", "feature-auth", "feature-ui", "develop"]
            .into_iter()
            .filter(|name| name != &branch_to_delete)
            .collect::<HashSet<_>>();
        let repo_branches = branch_list
            .update(cx, |branch_list, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker
                        .delegate
                        .repo
                        .as_ref()
                        .unwrap()
                        .update(cx, |repo, _cx| repo.branches())
                })
            })
            .await
            .unwrap()
            .unwrap();
        let repo_branches = repo_branches
            .iter()
            .map(|b| b.name())
            .collect::<HashSet<_>>();
        assert_eq!(&repo_branches, &expected_branches);

        branch_list.update(cx, move |branch_list, cx| {
            branch_list.picker.update(cx, move |picker, _cx| {
                assert_eq!(picker.delegate.matches.len(), 3);
                let branches = picker
                    .delegate
                    .matches
                    .iter()
                    .map(|be| be.name())
                    .collect::<HashSet<_>>();
                assert_eq!(branches, expected_branches);
            })
        });
    }

    #[gpui::test]
    async fn test_delete_remote_branch(cx: &mut TestAppContext) {
        init_test(cx);
        let (_project, repository) = init_fake_repository(cx).await;
        let branches = vec![
            create_test_branch("main", true, Some("origin"), Some(1000)),
            create_test_branch("feature-auth", false, Some("origin"), Some(900)),
            create_test_branch("feature-ui", false, Some("fork"), Some(800)),
            create_test_branch("develop", false, Some("private"), Some(700)),
        ];

        let branch_names = branches
            .iter()
            .map(|branch| branch.name().to_string())
            .collect::<Vec<String>>();
        let repo = repository.clone();
        cx.spawn(async move |mut cx| {
            for branch in branch_names {
                repo.update(&mut cx, |repo, _| repo.create_branch(branch, None))
                    .await
                    .unwrap()
                    .unwrap();
            }
        })
        .await;
        cx.run_until_parked();

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, repository.into(), branches, cx).await;
        let cx = &mut ctx;
        // Enable remote filter
        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                picker.delegate.branch_filter = BranchFilter::Remote;
            });
        });
        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        // Check matches, it should match all existing branches and no option to create new branch
        let branch_to_delete = branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                assert_eq!(picker.delegate.matches.len(), 4);
                let branch_to_delete = picker.delegate.matches.get(1).unwrap().name().to_string();
                picker.delegate.delete_at(1, window, cx);
                branch_to_delete
            })
        });
        cx.run_until_parked();

        let expected_branches = [
            "origin/main",
            "origin/feature-auth",
            "fork/feature-ui",
            "private/develop",
        ]
        .into_iter()
        .filter(|name| name != &branch_to_delete)
        .collect::<HashSet<_>>();
        let repo_branches = branch_list
            .update(cx, |branch_list, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker
                        .delegate
                        .repo
                        .as_ref()
                        .unwrap()
                        .update(cx, |repo, _cx| repo.branches())
                })
            })
            .await
            .unwrap()
            .unwrap();
        let repo_branches = repo_branches
            .iter()
            .map(|b| b.name())
            .collect::<HashSet<_>>();
        assert_eq!(&repo_branches, &expected_branches);

        // Check matches, it should match one less branch than before
        branch_list.update(cx, move |branch_list, cx| {
            branch_list.picker.update(cx, move |picker, _cx| {
                assert_eq!(picker.delegate.matches.len(), 3);
                let branches = picker
                    .delegate
                    .matches
                    .iter()
                    .map(|be| be.name())
                    .collect::<HashSet<_>>();
                assert_eq!(branches, expected_branches);
            })
        });
    }

    #[gpui::test]
    async fn test_title_bar_branch_entry_opens_branch_actions(cx: &mut TestAppContext) {
        init_test(cx);

        let branches = vec![
            create_test_branch("main", true, None, Some(1000)),
            create_test_branch_with_upstream("feature-auth", false, "origin", Some(900)),
        ];
        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::TitleBarPopover, None, branches, cx).await;
        let cx = &mut ctx;

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                picker.delegate.selected_index =
                    branch_match_index(&picker.delegate.matches, "feature-auth");
                picker.delegate.confirm(false, window, cx);

                assert!(matches!(
                    picker.delegate.state,
                    PickerState::BranchActions { ref branch }
                    if branch.name() == "feature-auth"
                ));
                assert_eq!(picker.delegate.matches.len(), 7);

                let action_labels = branch_action_labels(&picker.delegate.matches);

                assert_eq!(
                    action_labels,
                    vec![
                        ("Checkout".to_string(), false),
                        ("New Branch from 'feature-auth'...".to_string(), false),
                        ("Compare with current".to_string(), false),
                        ("Merge 'feature-auth' into 'main'".to_string(), false),
                        ("Rename...".to_string(), false),
                        ("Delete local".to_string(), false),
                        ("Delete with tracked branch".to_string(), false),
                    ]
                );
            })
        });
    }

    #[gpui::test]
    async fn test_title_bar_branch_list_is_grouped_like_idea(cx: &mut TestAppContext) {
        init_test(cx);

        let branches = vec![
            create_test_branch("main", true, None, Some(1000)),
            create_test_branch_with_upstream("feature-auth", false, "origin", Some(900)),
        ];
        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::TitleBarPopover, None, branches, cx).await;
        let cx = &mut ctx;

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                assert!(matches!(
                    picker.delegate.matches.first(),
                    Some(Entry::SectionHeader { label }) if label.as_ref() == "Recent"
                ));
                assert_eq!(picker.delegate.selected_index, 1);
                assert!(matches!(
                    picker.delegate.matches.get(3),
                    Some(Entry::SectionHeader { label }) if label.as_ref() == "Local"
                ));
            })
        });
    }

    #[gpui::test]
    async fn test_title_bar_delete_action_requires_confirmation(cx: &mut TestAppContext) {
        init_test(cx);

        let branches = vec![
            create_test_branch("main", true, None, Some(1000)),
            create_test_branch_with_upstream("feature-auth", false, "origin", Some(900)),
        ];
        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::TitleBarPopover, None, branches, cx).await;
        let cx = &mut ctx;

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                picker.delegate.selected_index =
                    branch_match_index(&picker.delegate.matches, "feature-auth");
                picker.delegate.confirm(false, window, cx);

                let delete_local_index = picker
                    .delegate
                    .matches
                    .iter()
                    .position(|entry| {
                        matches!(
                            entry,
                            Entry::BranchAction { entry }
                            if entry.label.as_ref() == "Delete local"
                        )
                    })
                    .expect("delete local action should be present");

                picker.delegate.selected_index = delete_local_index;
                picker.delegate.confirm(false, window, cx);

                assert!(matches!(
                    picker.delegate.state,
                    PickerState::ConfirmBranchAction { ref action }
                        if matches!(
                            action,
                            BranchAction::DeleteLocal { branch }
                            if branch.name() == "feature-auth"
                        )
                ));
                assert_eq!(
                    branch_action_labels(&picker.delegate.matches),
                    vec![
                        ("Confirm delete feature-auth".to_string(), false),
                        ("Cancel".to_string(), false),
                    ]
                );

                picker.delegate.selected_index = 1;
                picker.delegate.confirm(false, window, cx);

                assert!(matches!(
                    picker.delegate.state,
                    PickerState::BranchActions { ref branch }
                    if branch.name() == "feature-auth"
                ));
                assert!(picker.delegate.matches.iter().any(|entry| matches!(
                    entry,
                    Entry::BranchAction { entry }
                    if entry.label.as_ref() == "Delete local"
                )));
            })
        });
    }

    #[gpui::test]
    async fn test_merge_confirmation_entries_are_available_in_title_bar_popover(
        cx: &mut TestAppContext,
    ) {
        init_test(cx);

        let branches = vec![
            create_test_branch("main", true, None, Some(1000)),
            create_test_branch("feature-auth", false, None, Some(900)),
        ];
        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::TitleBarPopover, None, branches, cx).await;
        let cx = &mut ctx;

        branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                picker.delegate.open_branch_action_confirmation(
                    BranchAction::MergeIntoCurrent {
                        branch: create_test_branch("feature-auth", false, None, Some(900)),
                    },
                    window,
                    cx,
                );

                assert!(matches!(
                    picker.delegate.state,
                    PickerState::ConfirmBranchAction { ref action }
                        if matches!(
                            action,
                            BranchAction::MergeIntoCurrent { branch }
                            if branch.name() == "feature-auth"
                        )
                ));
                assert_eq!(
                    branch_action_labels(&picker.delegate.matches),
                    vec![
                        (
                            "Confirm merge feature-auth into main".to_string(),
                            false
                        ),
                        ("Cancel".to_string(), false),
                    ]
                );
            })
        });
    }

    #[gpui::test]
    async fn test_branch_filter_shows_all_then_remotes_and_applies_query(cx: &mut TestAppContext) {
        init_test(cx);

        let branches = vec![
            create_test_branch("main", true, Some("origin"), Some(1000)),
            create_test_branch("feature-auth", false, Some("fork"), Some(900)),
            create_test_branch("feature-ui", false, None, Some(800)),
            create_test_branch("develop", false, None, Some(700)),
        ];

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, None, branches, cx).await;
        let cx = &mut ctx;

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                assert_eq!(picker.delegate.matches.len(), 4);

                let branches = picker
                    .delegate
                    .matches
                    .iter()
                    .map(|be| be.name())
                    .collect::<HashSet<_>>();
                assert_eq!(
                    branches,
                    ["origin/main", "fork/feature-auth", "feature-ui", "develop"]
                        .into_iter()
                        .collect::<HashSet<_>>()
                );

                // Locals should be listed before remotes.
                let ordered = picker
                    .delegate
                    .matches
                    .iter()
                    .map(|be| be.name())
                    .collect::<Vec<_>>();
                assert_eq!(
                    ordered,
                    vec!["feature-ui", "develop", "origin/main", "fork/feature-auth"]
                );

                // Verify the last entry is NOT the "create new branch" option
                let last_match = picker.delegate.matches.last().unwrap();
                assert!(!last_match.is_new_branch());
                assert!(!last_match.is_new_url());
            })
        });

        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                picker.delegate.branch_filter = BranchFilter::Remote;
            })
        });

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    assert_eq!(picker.delegate.matches.len(), 2);
                    let branches = picker
                        .delegate
                        .matches
                        .iter()
                        .map(|be| be.name())
                        .collect::<HashSet<_>>();
                    assert_eq!(
                        branches,
                        ["origin/main", "fork/feature-auth"]
                            .into_iter()
                            .collect::<HashSet<_>>()
                    );

                    // Verify the last entry is NOT the "create new branch" option
                    let last_match = picker.delegate.matches.last().unwrap();
                    assert!(!last_match.is_new_url());
                    picker.delegate.branch_filter = BranchFilter::Remote;
                    picker
                        .delegate
                        .update_matches(String::from("fork"), window, cx)
                })
            })
            .await;
        cx.run_until_parked();

        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                // Should have 1 existing branch + 1 "create new branch" entry = 2 total
                assert_eq!(picker.delegate.matches.len(), 2);
                assert!(
                    picker
                        .delegate
                        .matches
                        .iter()
                        .any(|m| m.name() == "fork/feature-auth")
                );
                // Verify the last entry is the "create new branch" option
                let last_match = picker.delegate.matches.last().unwrap();
                assert!(last_match.is_new_branch());
            })
        });
    }

    #[gpui::test]
    async fn test_new_branch_creation_with_query(test_cx: &mut TestAppContext) {
        const MAIN_BRANCH: &str = "main";
        const FEATURE_BRANCH: &str = "feature";
        const NEW_BRANCH: &str = "new-feature-branch";

        init_test(test_cx);
        let (_project, repository) = init_fake_repository(test_cx).await;

        let branches = vec![
            create_test_branch(MAIN_BRANCH, true, None, Some(1000)),
            create_test_branch(FEATURE_BRANCH, false, None, Some(900)),
        ];

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, repository.into(), branches, test_cx)
                .await;
        let cx = &mut ctx;

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker
                        .delegate
                        .update_matches(NEW_BRANCH.to_string(), window, cx)
                })
            })
            .await;

        cx.run_until_parked();

        branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                let last_match = picker.delegate.matches.last().unwrap();
                assert!(last_match.is_new_branch());
                assert_eq!(last_match.name(), NEW_BRANCH);
                // State is NewBranch because no existing branches fuzzy-match the query
                assert!(matches!(picker.delegate.state, PickerState::NewBranch));
                picker.delegate.confirm(false, window, cx);
            })
        });
        cx.run_until_parked();

        let branches = branch_list
            .update(cx, |branch_list, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker
                        .delegate
                        .repo
                        .as_ref()
                        .unwrap()
                        .update(cx, |repo, _cx| repo.branches())
                })
            })
            .await
            .unwrap()
            .unwrap();

        let new_branch = branches
            .into_iter()
            .find(|branch| branch.name() == NEW_BRANCH)
            .expect("new-feature-branch should exist");
        assert_eq!(
            new_branch.ref_name.as_ref(),
            &format!("refs/heads/{NEW_BRANCH}"),
            "branch ref_name should not have duplicate refs/heads/ prefix"
        );
    }

    #[gpui::test]
    async fn test_remote_url_detection_https(cx: &mut TestAppContext) {
        init_test(cx);
        let (_project, repository) = init_fake_repository(cx).await;
        let branches = vec![create_test_branch("main", true, None, Some(1000))];

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, repository.into(), branches, cx).await;
        let cx = &mut ctx;

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    let query = "https://github.com/user/repo.git".to_string();
                    picker.delegate.update_matches(query, window, cx)
                })
            })
            .await;

        cx.run_until_parked();

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    let last_match = picker.delegate.matches.last().unwrap();
                    assert!(last_match.is_new_url());
                    assert!(matches!(picker.delegate.state, PickerState::NewRemote));
                    picker.delegate.confirm(false, window, cx);
                    assert_eq!(picker.delegate.matches.len(), 0);
                    if let PickerState::CreateRemote(remote_url) = &picker.delegate.state
                        && remote_url.as_ref() == "https://github.com/user/repo.git"
                    {
                    } else {
                        panic!("wrong picker state");
                    }
                    picker
                        .delegate
                        .update_matches("my_new_remote".to_string(), window, cx)
                })
            })
            .await;

        cx.run_until_parked();

        branch_list.update_in(cx, |branch_list, window, cx| {
            branch_list.picker.update(cx, |picker, cx| {
                assert_eq!(picker.delegate.matches.len(), 1);
                assert!(matches!(
                    picker.delegate.matches.first(),
                    Some(Entry::NewRemoteName { name, url })
                        if name == "my_new_remote" && url.as_ref() == "https://github.com/user/repo.git"
                ));
                picker.delegate.confirm(false, window, cx);
            })
        });
        cx.run_until_parked();

        // List remotes
        let remotes = branch_list
            .update(cx, |branch_list, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker
                        .delegate
                        .repo
                        .as_ref()
                        .unwrap()
                        .update(cx, |repo, _cx| repo.get_remotes(None, false))
                })
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            remotes,
            vec![Remote {
                name: SharedString::from("my_new_remote".to_string())
            }]
        );
    }

    #[gpui::test]
    async fn test_confirm_remote_url_transitions(cx: &mut TestAppContext) {
        init_test(cx);

        let branches = vec![create_test_branch("main_branch", true, None, Some(1000))];
        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, None, branches, cx).await;
        let cx = &mut ctx;

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    let query = "https://github.com/user/repo.git".to_string();
                    picker.delegate.update_matches(query, window, cx)
                })
            })
            .await;
        cx.run_until_parked();

        // Try to create a new remote but cancel in the middle of the process
        branch_list
            .update_in(cx, |branch_list, window, cx| {
                branch_list.picker.update(cx, |picker, cx| {
                    picker.delegate.selected_index = picker.delegate.matches.len() - 1;
                    picker.delegate.confirm(false, window, cx);

                    assert!(matches!(
                        picker.delegate.state,
                        PickerState::CreateRemote(_)
                    ));
                    if let PickerState::CreateRemote(ref url) = picker.delegate.state {
                        assert_eq!(url.as_ref(), "https://github.com/user/repo.git");
                    }
                    assert_eq!(picker.delegate.matches.len(), 0);
                    picker.delegate.dismissed(window, cx);
                    assert!(matches!(picker.delegate.state, PickerState::List));
                    let query = "main".to_string();
                    picker.delegate.update_matches(query, window, cx)
                })
            })
            .await;
        cx.run_until_parked();

        // Try to search a branch again to see if the state is restored properly
        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                // Should have 1 existing branch + 1 "create new branch" entry = 2 total
                assert_eq!(picker.delegate.matches.len(), 2);
                assert!(
                    picker
                        .delegate
                        .matches
                        .iter()
                        .any(|m| m.name() == "main_branch")
                );
                // Verify the last entry is the "create new branch" option
                let last_match = picker.delegate.matches.last().unwrap();
                assert!(last_match.is_new_branch());
            })
        });
    }

    #[gpui::test]
    async fn test_confirm_remote_url_does_not_dismiss(cx: &mut TestAppContext) {
        const REMOTE_URL: &str = "https://github.com/user/repo.git";

        init_test(cx);
        let branches = vec![create_test_branch("main", true, None, Some(1000))];

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, None, branches, cx).await;
        let cx = &mut ctx;

        let subscription = cx.update(|_, cx| {
            cx.subscribe(&branch_list, |_, _: &DismissEvent, _| {
                panic!("DismissEvent should not be emitted when confirming a remote URL");
            })
        });

        branch_list
            .update_in(cx, |branch_list, window, cx| {
                window.focus(&branch_list.picker_focus_handle, cx);
                assert!(
                    branch_list.picker_focus_handle.is_focused(window),
                    "Branch picker should be focused when selecting an entry"
                );

                branch_list.picker.update(cx, |picker, cx| {
                    picker
                        .delegate
                        .update_matches(REMOTE_URL.to_string(), window, cx)
                })
            })
            .await;

        cx.run_until_parked();

        branch_list.update_in(cx, |branch_list, window, cx| {
            // Re-focus the picker since workspace initialization during run_until_parked
            window.focus(&branch_list.picker_focus_handle, cx);

            branch_list.picker.update(cx, |picker, cx| {
                let last_match = picker.delegate.matches.last().unwrap();
                assert!(last_match.is_new_url());
                assert!(matches!(picker.delegate.state, PickerState::NewRemote));

                picker.delegate.confirm(false, window, cx);

                assert!(
                    matches!(picker.delegate.state, PickerState::CreateRemote(ref url) if url.as_ref() == REMOTE_URL),
                    "State should transition to CreateRemote with the URL"
                );
            });

            assert!(
                branch_list.picker_focus_handle.is_focused(window),
                "Branch list picker should still be focused after confirming remote URL"
            );
        });

        cx.run_until_parked();

        drop(subscription);
    }

    #[gpui::test(iterations = 10)]
    async fn test_empty_query_displays_all_branches(mut rng: StdRng, cx: &mut TestAppContext) {
        init_test(cx);
        let branch_count = rng.random_range(13..540);

        let branches: Vec<Branch> = (0..branch_count)
            .map(|i| create_test_branch(&format!("branch-{:02}", i), i == 0, None, Some(i * 100)))
            .collect();

        let (branch_list, mut ctx) =
            init_branch_list_test(BranchListStyle::Modal, None, branches, cx).await;
        let cx = &mut ctx;

        update_branch_list_matches_with_empty_query(&branch_list, cx).await;

        branch_list.update(cx, |branch_list, cx| {
            branch_list.picker.update(cx, |picker, _cx| {
                assert_eq!(picker.delegate.matches.len(), branch_count as usize);
            })
        });
    }
}
