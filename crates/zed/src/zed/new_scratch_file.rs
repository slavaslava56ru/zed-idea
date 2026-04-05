use anyhow::Context as _;
use editor::{Editor, MultiBufferOffset};
use file_icons::FileIcons;
use fs::Fs;
use fuzzy::{StringMatch, StringMatchCandidate, match_strings};
use gpui::{
    App, Context, DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, Global,
    ParentElement, Render, Styled, Subscription, WeakEntity, Window, rems,
};
use language::LanguageRegistry;
use picker::{Picker, PickerDelegate};
use std::{
    collections::{HashSet, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
};
use ui::{
    Color, HighlightedLabel, Icon, IconName, Label, LabelSize, ListItem, ListItemSpacing,
    prelude::*,
};
use ui_input::ErasedEditor;
use util::ResultExt;
use workspace::{
    ModalView, OpenOptions, OpenVisible, Workspace, notifications::DetachAndPromptErr,
};

const SCRATCH_HISTORY_LIMIT: usize = 4;
const DEFAULT_SCRATCH_EXTENSION: &str = "txt";
const PLAIN_TEXT_LANGUAGE_NAME: &str = "Plain Text";

#[derive(Default)]
struct ScratchFileHistory {
    recent_type_ids: VecDeque<String>,
}

impl Global for ScratchFileHistory {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ScratchFileType {
    id: String,
    display_name: String,
    language_name: String,
    path_suffix: String,
    use_separator_dot: bool,
    representative_path: String,
    primary_for_language: bool,
}

impl ScratchFileType {
    fn file_name_for_index(&self, index: usize) -> String {
        let separator = if self.use_separator_dot { "." } else { "" };
        format!("scratch-{index}{separator}{}", self.path_suffix)
    }

    fn matches_file_name(&self, file_name: &str) -> bool {
        if is_special_file_name_suffix(&self.path_suffix) {
            file_name == self.path_suffix
        } else {
            file_name == self.path_suffix || file_name.ends_with(&format!(".{}", self.path_suffix))
        }
    }

    fn icon(&self, cx: &App) -> Icon {
        FileIcons::get_icon(Path::new(&self.representative_path), cx)
            .map(Icon::from_path)
            .unwrap_or_else(|| Icon::new(IconName::File))
            .color(Color::Muted)
    }
}

pub(crate) fn toggle(workspace: &mut Workspace, window: &mut Window, cx: &mut Context<Workspace>) {
    ensure_history(cx);

    let workspace_handle = cx.entity().downgrade();
    let language_registry = workspace.app_state().languages.clone();
    let preferred_type_id = preferred_scratch_type_id(workspace, &language_registry, cx);

    workspace.toggle_modal(window, cx, move |window, cx| {
        NewScratchFileModal::new(
            workspace_handle.clone(),
            language_registry.clone(),
            preferred_type_id.clone(),
            window,
            cx,
        )
    });
}

fn ensure_history(cx: &mut App) {
    if cx.try_global::<ScratchFileHistory>().is_none() {
        cx.set_global(ScratchFileHistory::default());
    }
}

fn recent_type_ids(cx: &App) -> Vec<String> {
    cx.try_global::<ScratchFileHistory>()
        .map(|history| history.recent_type_ids.iter().cloned().collect())
        .unwrap_or_default()
}

fn remember_type_id(type_id: &str, cx: &mut App) {
    ensure_history(cx);
    let type_id = type_id.to_string();
    cx.update_global(|history: &mut ScratchFileHistory, _| {
        history
            .recent_type_ids
            .retain(|existing| existing != &type_id);
        history.recent_type_ids.push_front(type_id);
        history.recent_type_ids.truncate(SCRATCH_HISTORY_LIMIT);
    });
}

pub struct NewScratchFileModal {
    picker: Entity<Picker<ScratchFileSelectorDelegate>>,
    _subscription: Subscription,
}

impl NewScratchFileModal {
    fn new(
        workspace: WeakEntity<Workspace>,
        language_registry: Arc<LanguageRegistry>,
        preferred_type_id: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let scratch_file_types = build_scratch_file_types(&language_registry);
        let recent_type_ids = recent_type_ids(cx);
        let delegate = ScratchFileSelectorDelegate::new(
            workspace,
            language_registry,
            scratch_file_types,
            recent_type_ids,
            preferred_type_id,
        );
        let picker = cx.new(|cx| {
            Picker::uniform_list(delegate, window, cx)
                .modal(false)
                .max_height(Some(rems(22.).into()))
        });
        let _subscription = cx.subscribe(&picker, |_, _, _: &DismissEvent, cx| {
            cx.emit(DismissEvent);
        });
        Self {
            picker,
            _subscription,
        }
    }
}

impl Focusable for NewScratchFileModal {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.picker.focus_handle(cx)
    }
}

impl EventEmitter<DismissEvent> for NewScratchFileModal {}
impl ModalView for NewScratchFileModal {}

impl Render for NewScratchFileModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .w(rems(41.))
            .overflow_hidden()
            .rounded_lg()
            .border_1()
            .border_color(cx.theme().colors().border)
            .bg(cx.theme().colors().surface_background)
            .elevation_3(cx)
            .child(
                h_flex()
                    .h_12()
                    .justify_center()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().colors().border_variant)
                    .child(Label::new("New Scratch File").size(LabelSize::Large)),
            )
            .child(self.picker.clone())
            .on_mouse_down_out(cx.listener(|this, _, window, cx| {
                this.picker.update(cx, |picker, cx| {
                    picker.cancel(&Default::default(), window, cx);
                });
            }))
    }
}

struct ScratchFileSelectorDelegate {
    workspace: WeakEntity<Workspace>,
    language_registry: Arc<LanguageRegistry>,
    scratch_file_types: Vec<ScratchFileType>,
    candidates: Vec<StringMatchCandidate>,
    matches: Vec<StringMatch>,
    selected_index: usize,
    preferred_type_id: Option<String>,
    recent_type_ids: Vec<String>,
    separator_after_index: Option<usize>,
}

impl ScratchFileSelectorDelegate {
    fn new(
        workspace: WeakEntity<Workspace>,
        language_registry: Arc<LanguageRegistry>,
        scratch_file_types: Vec<ScratchFileType>,
        recent_type_ids: Vec<String>,
        preferred_type_id: Option<String>,
    ) -> Self {
        let candidates = scratch_file_types
            .iter()
            .enumerate()
            .map(|(candidate_id, scratch_file_type)| {
                StringMatchCandidate::new(candidate_id, &scratch_file_type.display_name)
            })
            .collect();

        Self {
            workspace,
            language_registry,
            scratch_file_types,
            candidates,
            matches: Vec::new(),
            selected_index: 0,
            preferred_type_id,
            recent_type_ids,
            separator_after_index: None,
        }
    }

    fn ordered_candidate_ids_for_empty_query(&self) -> (Vec<usize>, Option<usize>) {
        let mut ordered_ids = Vec::with_capacity(self.scratch_file_types.len());
        let mut seen_ids = HashSet::with_capacity(self.scratch_file_types.len());

        let mut push_type_id = |type_id: &str| {
            if let Some(candidate_id) = self
                .scratch_file_types
                .iter()
                .position(|scratch_file_type| scratch_file_type.id == type_id)
                && seen_ids.insert(candidate_id)
            {
                ordered_ids.push(candidate_id);
            }
        };

        for recent_type_id in &self.recent_type_ids {
            push_type_id(recent_type_id);
        }

        if let Some(preferred_type_id) = &self.preferred_type_id {
            push_type_id(preferred_type_id);
        }

        if let Some(plain_text_type_id) = self
            .scratch_file_types
            .iter()
            .find(|scratch_file_type| {
                scratch_file_type.primary_for_language
                    && scratch_file_type.language_name == PLAIN_TEXT_LANGUAGE_NAME
            })
            .map(|scratch_file_type| scratch_file_type.id.as_str())
        {
            push_type_id(plain_text_type_id);
        }

        let top_section_len = ordered_ids.len();

        for candidate_id in 0..self.scratch_file_types.len() {
            if seen_ids.insert(candidate_id) {
                ordered_ids.push(candidate_id);
            }
        }

        let separator_after_index = (top_section_len > 0 && top_section_len < ordered_ids.len())
            .then_some(top_section_len - 1);

        (ordered_ids, separator_after_index)
    }
}

impl PickerDelegate for ScratchFileSelectorDelegate {
    type ListItem = ListItem;

    fn placeholder_text(&self, _window: &mut Window, _cx: &mut App) -> Arc<str> {
        "Search file types...".into()
    }

    fn no_matches_text(&self, _window: &mut Window, _cx: &mut App) -> Option<SharedString> {
        Some("No matching file types".into())
    }

    fn render_editor(
        &self,
        editor: &Arc<dyn ErasedEditor>,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> Div {
        div()
            .h_0()
            .overflow_hidden()
            .opacity(0.)
            .child(editor.render(window, cx))
    }

    fn match_count(&self) -> usize {
        self.matches.len()
    }

    fn selected_index(&self) -> usize {
        self.selected_index
    }

    fn separators_after_indices(&self) -> Vec<usize> {
        self.separator_after_index.into_iter().collect()
    }

    fn set_selected_index(
        &mut self,
        ix: usize,
        _window: &mut Window,
        _cx: &mut Context<Picker<Self>>,
    ) {
        self.selected_index = ix;
    }

    fn update_matches(
        &mut self,
        query: String,
        window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> gpui::Task<()> {
        let background = cx.background_executor().clone();
        let candidates = self.candidates.clone();
        let preferred_type_id = self.preferred_type_id.clone();
        let query_is_empty = query.is_empty();
        let (ordered_candidate_ids, separator_after_index) =
            self.ordered_candidate_ids_for_empty_query();

        cx.spawn_in(window, async move |this, cx| {
            let matches = if query_is_empty {
                ordered_candidate_ids
                    .into_iter()
                    .map(|candidate_id| {
                        let candidate = &candidates[candidate_id];
                        StringMatch {
                            candidate_id,
                            string: candidate.string.clone(),
                            positions: Vec::new(),
                            score: 0.0,
                        }
                    })
                    .collect()
            } else {
                match_strings(
                    &candidates,
                    &query,
                    false,
                    true,
                    500,
                    &Default::default(),
                    background,
                )
                .await
            };

            this.update_in(cx, |this, window, cx| {
                this.delegate.separator_after_index =
                    query_is_empty.then_some(separator_after_index).flatten();
                this.delegate.matches = matches;

                let selected_index = if query_is_empty {
                    preferred_type_id
                        .as_ref()
                        .and_then(|preferred_type_id| {
                            this.delegate.matches.iter().position(|mat| {
                                this.delegate
                                    .scratch_file_types
                                    .get(mat.candidate_id)
                                    .is_some_and(|scratch_file_type| {
                                        scratch_file_type.id == *preferred_type_id
                                    })
                            })
                        })
                        .unwrap_or(0)
                } else {
                    0
                };

                this.delegate.selected_index = selected_index;
                this.set_selected_index(selected_index, None, true, window, cx);
            })
            .log_err();
        })
    }

    fn confirm(&mut self, _secondary: bool, window: &mut Window, cx: &mut Context<Picker<Self>>) {
        let Some(mat) = self.matches.get(self.selected_index) else {
            return;
        };
        let Some(scratch_file_type) = self.scratch_file_types.get(mat.candidate_id).cloned() else {
            return;
        };

        remember_type_id(&scratch_file_type.id, cx);
        create_and_open_scratch_file(
            self.workspace.clone(),
            self.language_registry.clone(),
            scratch_file_type,
            window,
            cx,
        );
        cx.emit(DismissEvent);
    }

    fn dismissed(&mut self, _window: &mut Window, _cx: &mut Context<Picker<Self>>) {}

    fn render_match(
        &self,
        ix: usize,
        selected: bool,
        _window: &mut Window,
        cx: &mut Context<Picker<Self>>,
    ) -> Option<Self::ListItem> {
        let mat = self.matches.get(ix)?;
        let scratch_file_type = self.scratch_file_types.get(mat.candidate_id)?;

        Some(
            ListItem::new(ix)
                .inset(true)
                .spacing(ListItemSpacing::Sparse)
                .toggle_state(selected)
                .start_slot::<Icon>(Some(scratch_file_type.icon(cx)))
                .child(HighlightedLabel::new(
                    scratch_file_type.display_name.clone(),
                    mat.positions.clone(),
                )),
        )
    }
}

fn preferred_scratch_type_id(
    workspace: &Workspace,
    language_registry: &Arc<LanguageRegistry>,
    cx: &App,
) -> Option<String> {
    let scratch_file_types = build_scratch_file_types(language_registry);
    let active_editor = workspace.active_item_as::<Editor>(cx)?;
    let active_editor = active_editor.read(cx);
    let buffer = active_editor.buffer().read(cx).as_singleton()?;
    let buffer = buffer.read(cx);
    let file_name = buffer.file().and_then(|file| {
        file.full_path(cx)
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .map(|file_name| file_name.to_string())
    });
    let language_name = active_editor
        .buffer()
        .read(cx)
        .language_at(MultiBufferOffset(0), cx)
        .map(|language| language.name().as_ref().to_string());

    file_name
        .and_then(|file_name| {
            scratch_file_types
                .iter()
                .find(|scratch_file_type| {
                    scratch_file_type.primary_for_language
                        && scratch_file_type.matches_file_name(&file_name)
                })
                .map(|scratch_file_type| scratch_file_type.id.clone())
        })
        .or_else(|| {
            language_name.and_then(|language_name| {
                scratch_file_types
                    .iter()
                    .find(|scratch_file_type| {
                        scratch_file_type.primary_for_language
                            && scratch_file_type.language_name == language_name
                    })
                    .map(|scratch_file_type| scratch_file_type.id.clone())
            })
        })
}

fn build_scratch_file_types(language_registry: &Arc<LanguageRegistry>) -> Vec<ScratchFileType> {
    let mut scratch_file_types = Vec::new();
    let mut used_ids = HashSet::new();

    for language_name in language_registry.language_names() {
        let Some(available_language) = language_registry
            .available_language_for_name(language_name.as_ref())
            .filter(|language| !language.hidden())
        else {
            continue;
        };

        let language_name = available_language.name().as_ref().to_string();
        let matcher = available_language.matcher();
        let extension_suffix = matcher
            .path_suffixes
            .iter()
            .find(|suffix| is_regular_extension_suffix(suffix))
            .cloned();
        let special_suffixes = matcher
            .path_suffixes
            .iter()
            .filter(|suffix| is_special_file_name_suffix(suffix))
            .cloned()
            .collect::<Vec<_>>();

        let primary_suffix = extension_suffix
            .clone()
            .or_else(|| special_suffixes.first().cloned())
            .unwrap_or_else(|| DEFAULT_SCRATCH_EXTENSION.to_string());

        let primary_type = scratch_file_type_for_suffix(&language_name, &primary_suffix, true);
        if used_ids.insert(primary_type.id.clone()) {
            scratch_file_types.push(primary_type);
        }

        if extension_suffix.is_some() {
            for special_suffix in special_suffixes {
                if special_suffix != primary_suffix {
                    let special_type =
                        scratch_file_type_for_suffix(&language_name, &special_suffix, false);
                    if used_ids.insert(special_type.id.clone()) {
                        scratch_file_types.push(special_type);
                    }
                }
            }
        }
    }

    scratch_file_types
        .sort_by_cached_key(|scratch_file_type| scratch_file_type.display_name.to_lowercase());
    scratch_file_types
}

fn scratch_file_type_for_suffix(
    language_name: &str,
    path_suffix: &str,
    primary_for_language: bool,
) -> ScratchFileType {
    let use_separator_dot = !path_suffix.starts_with('.');
    let display_name = if is_special_file_name_suffix(path_suffix) {
        special_display_name(path_suffix, language_name)
    } else {
        language_name.to_string()
    };
    let representative_path = if is_special_file_name_suffix(path_suffix) {
        path_suffix.to_string()
    } else {
        format!("scratch.{path_suffix}")
    };
    let id = format!("{language_name}::{path_suffix}");

    ScratchFileType {
        id,
        display_name,
        language_name: language_name.to_string(),
        path_suffix: path_suffix.to_string(),
        use_separator_dot,
        representative_path,
        primary_for_language,
    }
}

fn special_display_name(path_suffix: &str, language_name: &str) -> String {
    if path_suffix.eq_ignore_ascii_case(language_name) {
        path_suffix.to_string()
    } else {
        format!("{path_suffix} ({language_name})")
    }
}

fn is_regular_extension_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && !suffix.starts_with('.')
        && suffix.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn is_special_file_name_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && (suffix.starts_with('.') || suffix.bytes().any(|byte| byte.is_ascii_uppercase()))
}

fn create_and_open_scratch_file(
    workspace: WeakEntity<Workspace>,
    language_registry: Arc<LanguageRegistry>,
    scratch_file_type: ScratchFileType,
    window: &mut Window,
    cx: &mut Context<Picker<ScratchFileSelectorDelegate>>,
) {
    cx.spawn_in(window, async move |_, cx| {
        let open_scratch_task = workspace
            .update_in(cx, |workspace, window, cx| {
                workspace.with_local_or_wsl_workspace(window, cx, move |workspace, window, cx| {
                    let fs = workspace.app_state().fs.clone();
                    let project = workspace.project().clone();
                    let language_registry = language_registry.clone();
                    let scratch_file_type = scratch_file_type.clone();

                    cx.spawn_in(window, async move |workspace, cx| {
                        let scratch_root = project
                            .update(cx, |project, cx| {
                                project.try_windows_path_to_wsl(paths::data_dir().as_path(), cx)
                            })
                            .await?;
                        let scratch_dir = scratch_root.join("scratch");
                        fs.create_dir(&scratch_dir).await?;

                        let _scratch_worktree = project
                            .update(cx, |project, cx| {
                                project.find_or_create_worktree(&scratch_dir, true, cx)
                            })
                            .await?;

                        let scratch_path =
                            next_scratch_file_path(fs.as_ref(), &scratch_dir, &scratch_file_type)
                                .await?;
                        fs.create_file(&scratch_path, Default::default()).await?;
                        let scratch_path =
                            fs.canonicalize(&scratch_path).await.unwrap_or(scratch_path);

                        let opened_item = workspace
                            .update_in(cx, |workspace, window, cx| {
                                workspace.open_abs_path(
                                    scratch_path,
                                    OpenOptions {
                                        visible: Some(OpenVisible::None),
                                        ..Default::default()
                                    },
                                    window,
                                    cx,
                                )
                            })?
                            .await?;

                        let language = language_registry
                            .language_for_name(&scratch_file_type.language_name)
                            .await?;
                        workspace.update_in(cx, |_, _, cx| {
                            let editor = opened_item
                                .act_as::<Editor>(cx)
                                .context("scratch file should open in an editor")?;
                            let buffer = editor
                                .read(cx)
                                .active_buffer(cx)
                                .context("scratch file should have an active buffer")?;
                            project.update(cx, |project, cx| {
                                project.set_language_for_buffer(&buffer, language, cx);
                            });
                            anyhow::Ok(())
                        })??;

                        anyhow::Ok(())
                    })
                })
            })?
            .await?;
        open_scratch_task.await?;
        anyhow::Ok(())
    })
    .detach_and_prompt_err("Failed to create scratch file", window, cx, |_, _, _| None);
}

async fn next_scratch_file_path(
    fs: &dyn Fs,
    scratch_dir: &Path,
    scratch_file_type: &ScratchFileType,
) -> anyhow::Result<PathBuf> {
    for index in 1.. {
        let scratch_path = scratch_dir.join(scratch_file_type.file_name_for_index(index));
        if !fs.is_file(&scratch_path).await {
            return Ok(scratch_path);
        }
    }

    unreachable!("scratch file path search should always find an available file name")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_file_names_handle_extensions_and_dotfiles() {
        let json_type = ScratchFileType {
            id: "JSON::json".to_string(),
            display_name: "JSON".to_string(),
            language_name: "JSON".to_string(),
            path_suffix: "json".to_string(),
            use_separator_dot: true,
            representative_path: "scratch.json".to_string(),
            primary_for_language: true,
        };
        let gitignore_type = ScratchFileType {
            id: "GitIgnore::.gitignore".to_string(),
            display_name: ".gitignore (GitIgnore)".to_string(),
            language_name: "GitIgnore".to_string(),
            path_suffix: ".gitignore".to_string(),
            use_separator_dot: false,
            representative_path: ".gitignore".to_string(),
            primary_for_language: true,
        };

        assert_eq!(json_type.file_name_for_index(3), "scratch-3.json");
        assert_eq!(gitignore_type.file_name_for_index(7), "scratch-7.gitignore");
    }

    #[test]
    fn special_display_name_avoids_duplicate_language_names() {
        assert_eq!(
            special_display_name("Dockerfile", "Dockerfile"),
            "Dockerfile"
        );
        assert_eq!(
            special_display_name(".gitignore", "GitIgnore"),
            ".gitignore (GitIgnore)"
        );
    }

    #[test]
    fn file_name_matching_uses_exact_names_for_special_suffixes() {
        let dockerfile_type = scratch_file_type_for_suffix("Dockerfile", "Dockerfile", true);
        let json_type = scratch_file_type_for_suffix("JSON", "json", true);

        assert!(dockerfile_type.matches_file_name("Dockerfile"));
        assert!(!dockerfile_type.matches_file_name("foo.Dockerfile"));
        assert!(json_type.matches_file_name("scratch-1.json"));
        assert!(json_type.matches_file_name("json"));
    }
}
