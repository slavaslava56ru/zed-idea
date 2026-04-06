use anyhow::Result;
use buffer_diff::BufferDiff;
use futures::{FutureExt, select_biased};
use gpui::{
    AnyElement, App, AppContext as _, AsyncApp, Context, DismissEvent, Entity, EventEmitter,
    FocusHandle, Focusable, IntoElement, MouseDownEvent, Render, Subscription, Task,
    UniformListScrollHandle, Window, anchored, deferred, px, uniform_list,
};
use language::{Buffer, BufferEvent, Capability};
use project::Project;
use settings::DiffViewStyle;
use std::{
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
    time::Duration,
};
use time::{OffsetDateTime, UtcOffset, macros::format_description};
use ui::{
    Color, ContextMenu, Icon, IconButtonShape, IconName, Label, ListItem, SharedString, Tooltip,
    WithScrollbar, prelude::*,
};
use util::{ResultExt, paths::PathExt as _};
use workspace::ModalView;
use workspace::Workspace;

use crate::{
    Autoscroll, CurrentLineHighlight, Editor, MultiBuffer, SplittableEditor, persistence::EditorDb,
};

const LOCAL_HISTORY_ENTRY_LIMIT: i64 = 100;
const DIFF_RECALCULATE_DEBOUNCE: Duration = Duration::from_millis(250);
const HISTORY_REFRESH_DEBOUNCE: Duration = Duration::from_millis(150);
const SIDEBAR_WIDTH: f32 = 280.0;

#[derive(Clone, Debug, PartialEq, Eq)]
struct LocalHistoryEntry {
    id: i64,
    saved_at_unix_ms: i64,
}

pub(crate) fn open_local_history(
    workspace: &mut Workspace,
    window: &mut Window,
    cx: &mut Context<Workspace>,
) {
    let Some(editor) = workspace.active_item_as::<Editor>(cx) else {
        return;
    };

    let Some(current_buffer) = editor.read(cx).buffer().read(cx).as_singleton() else {
        return;
    };
    let current_buffer = current_buffer.clone();

    let Some(abs_path) = current_buffer_path(&current_buffer, cx) else {
        return;
    };

    let current_text = current_buffer.read(cx).snapshot().text().to_string();
    let entries = visible_entries(load_entries(&abs_path, cx), &current_text, cx);
    let initial_snapshot_text = entries
        .first()
        .and_then(|entry| load_entry_contents(entry.id, cx))
        .unwrap_or_else(|| current_text.clone());
    let selected_entry_id = entries.first().map(|entry| entry.id);
    let project = workspace.project().clone();
    let workspace_handle = cx.entity();

    workspace.toggle_modal(window, cx, |window, cx| {
        LocalHistoryModal::new(
            current_buffer,
            abs_path,
            entries,
            selected_entry_id,
            initial_snapshot_text,
            project,
            workspace_handle,
            window,
            cx,
        )
    });
}

pub(crate) async fn save_local_history_snapshot(
    buffer: Entity<Buffer>,
    cx: &mut AsyncApp,
) -> Result<()> {
    let Some((abs_path, contents)) = buffer.read_with(cx, |buffer, cx| {
        let file = buffer.file()?;
        if file.is_private() {
            return None;
        }

        Some((
            file.full_path(cx).to_path_buf(),
            buffer.snapshot().text().to_string(),
        ))
    }) else {
        return Ok(());
    };

    let now = OffsetDateTime::now_utc();
    let saved_at_unix_ms = now.unix_timestamp().saturating_mul(1000) + i64::from(now.millisecond());
    let abs_path = abs_path.to_string_lossy().into_owned();

    let editor_db = cx.update(|cx| EditorDb::global(cx));
    editor_db
        .save_local_history_entry(
            abs_path,
            saved_at_unix_ms,
            contents,
            LOCAL_HISTORY_ENTRY_LIMIT,
        )
        .await
}

pub struct LocalHistoryModal {
    abs_path: PathBuf,
    current_buffer: Entity<Buffer>,
    snapshot_buffer: Entity<Buffer>,
    diff_editor: Entity<SplittableEditor>,
    entries: Vec<LocalHistoryEntry>,
    selected_entry_id: Option<i64>,
    entry_context_menu: Option<(Entity<ContextMenu>, Subscription, i64)>,
    scroll_handle: UniformListScrollHandle,
    should_center_first_change: bool,
    buffer_changes_tx: watch::Sender<()>,
    history_refresh_tx: watch::Sender<()>,
    _subscriptions: Vec<Subscription>,
    _recalculate_diff_task: Task<Result<()>>,
    _refresh_history_task: Task<Result<()>>,
}

impl LocalHistoryModal {
    #[allow(clippy::too_many_arguments)]
    fn new(
        current_buffer: Entity<Buffer>,
        abs_path: PathBuf,
        entries: Vec<LocalHistoryEntry>,
        selected_entry_id: Option<i64>,
        initial_snapshot_text: String,
        project: Entity<Project>,
        workspace: Entity<Workspace>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let snapshot_buffer = build_snapshot_buffer(&current_buffer, initial_snapshot_text, cx);
        let current_snapshot = current_buffer.read(cx).snapshot();
        let diff = cx.new(|cx| BufferDiff::new(&current_snapshot.text, cx));

        let multibuffer = cx.new(|cx| {
            let mut multibuffer = MultiBuffer::singleton(current_buffer.clone(), cx);
            multibuffer.add_diff(diff.clone(), cx);
            multibuffer
        });

        let diff_editor = cx.new(|cx| {
            let mut splittable = SplittableEditor::new(
                DiffViewStyle::Split,
                multibuffer,
                project,
                workspace,
                window,
                cx,
            );
            splittable.set_lhs_show_headers(false);
            splittable.rhs_editor().update(cx, |editor, cx| {
                editor.show_local_selections = true;
                editor.set_current_line_highlight(Some(CurrentLineHighlight::None));
                editor.set_show_cursor_when_unfocused(false, cx);
                editor.set_show_diff_decorations(true, cx);
                editor.set_show_git_diff_gutter(false, cx);
                editor.set_always_show_diff_hunk_controls(false, cx);
                editor.set_render_diff_hunk_controls(
                    Arc::new(|_, _, _, _, _, _, _, _| gpui::Empty.into_any_element()),
                    cx,
                );
                editor.start_temporary_diff_override();
                editor.disable_diagnostics(cx);
                editor.set_expand_all_diff_hunks(cx);
            });
            splittable
        });
        cx.defer_in(window, {
            let diff_editor = diff_editor.clone();
            move |_, window, cx| {
                diff_editor.update(cx, |splittable, cx| {
                    if !splittable.is_split() {
                        splittable.split(window, cx);
                    }

                    if let Some(lhs_editor) = splittable.lhs_editor() {
                        lhs_editor.update(cx, |editor, cx| {
                            editor.show_local_selections = true;
                            editor.set_current_line_highlight(Some(CurrentLineHighlight::None));
                            editor.set_show_cursor_when_unfocused(false, cx);
                            editor.set_show_git_diff_gutter(false, cx);
                            editor.set_always_show_diff_hunk_controls(true, cx);
                            editor.set_render_diff_hunk_controls(
                                Arc::new(
                                    |row,
                                     _,
                                     hunk_range,
                                     is_created_file,
                                     line_height,
                                     editor,
                                     _,
                                     _| {
                                        h_flex()
                                            .h(line_height)
                                            .items_center()
                                            .justify_center()
                                            .px_0p5()
                                            .child(
                                                IconButton::new(
                                                    ("apply-local-history-hunk", row as u64),
                                                    IconName::ChevronRight,
                                                )
                                                .shape(IconButtonShape::Square)
                                                .size(ButtonSize::Compact)
                                                .icon_size(IconSize::Small)
                                                .icon_color(Color::Accent)
                                                .disabled(is_created_file)
                                                .tooltip(Tooltip::text(
                                                    "Apply change from snapshot",
                                                ))
                                                .on_click({
                                                    let editor = editor.clone();
                                                    move |_event, window, cx| {
                                                        editor.update(cx, |editor, cx| {
                                                            editor.restore_diff_hunk_by_range(
                                                                hunk_range.clone(),
                                                                window,
                                                                cx,
                                                            );
                                                        });
                                                    }
                                                }),
                                            )
                                            .into_any_element()
                                    },
                                ),
                                cx,
                            );
                        });
                    }

                    splittable.rhs_editor().update(cx, |editor, cx| {
                        editor.show_local_selections = true;
                        editor.set_current_line_highlight(Some(CurrentLineHighlight::None));
                        editor.set_show_cursor_when_unfocused(false, cx);
                        editor.set_show_diff_decorations(true, cx);
                        editor.set_show_git_diff_gutter(false, cx);
                        editor.set_always_show_diff_hunk_controls(false, cx);
                        editor.set_render_diff_hunk_controls(
                            Arc::new(|_, _, _, _, _, _, _, _| gpui::Empty.into_any_element()),
                            cx,
                        );
                        editor.set_scroll_position(gpui::Point::default(), window, cx);
                    });

                    splittable
                        .rhs_editor()
                        .read(cx)
                        .focus_handle(cx)
                        .focus(window, cx);
                });
            }
        });

        let (buffer_changes_tx, mut buffer_changes_rx) = watch::channel(());
        let (history_refresh_tx, mut history_refresh_rx) = watch::channel(());
        let mut subscriptions = Vec::new();

        subscriptions.push(
            cx.subscribe(
                &current_buffer,
                |this, _, event: &BufferEvent, cx| match event {
                    BufferEvent::Edited { .. }
                    | BufferEvent::LanguageChanged(_)
                    | BufferEvent::Reparsed
                    | BufferEvent::Reloaded => {
                        this.sync_snapshot_language(cx);
                        this.notify_buffer_changes();
                    }
                    BufferEvent::Saved | BufferEvent::FileHandleChanged => {
                        this.sync_snapshot_language(cx);
                        this.notify_buffer_changes();
                        this.notify_history_refresh();
                    }
                    _ => {}
                },
            ),
        );

        subscriptions.push(
            cx.subscribe(
                &snapshot_buffer,
                |this, _, event: &BufferEvent, _| match event {
                    BufferEvent::Edited { .. }
                    | BufferEvent::LanguageChanged(_)
                    | BufferEvent::Reparsed
                    | BufferEvent::Reloaded => {
                        this.notify_buffer_changes();
                    }
                    _ => {}
                },
            ),
        );

        let recalculate_diff_task = cx.spawn(async move |this, cx| {
            while buffer_changes_rx.recv().await.is_ok() {
                loop {
                    let mut timer = cx
                        .background_executor()
                        .timer(DIFF_RECALCULATE_DEBOUNCE)
                        .fuse();
                    let mut recv = pin!(buffer_changes_rx.recv().fuse());
                    select_biased! {
                        _ = timer => break,
                        _ = recv => continue,
                    }
                }

                let (current_snapshot, snapshot_snapshot, language_registry) =
                    this.update(cx, |this, cx| {
                        (
                            this.current_buffer.read(cx).snapshot(),
                            this.snapshot_buffer.read(cx).snapshot(),
                            this.current_buffer.read(cx).language_registry(),
                        )
                    })?;

                let language = current_snapshot.language().cloned();
                let update = diff
                    .update(cx, |diff, cx| {
                        diff.update_diff(
                            current_snapshot.text.clone(),
                            Some(Arc::from(snapshot_snapshot.text().as_str())),
                            Some(true),
                            language.clone(),
                            cx,
                        )
                    })
                    .await;

                diff.update(cx, |diff, cx| {
                    diff.language_changed(language, language_registry, cx);
                    diff.set_snapshot(update, &current_snapshot.text, cx)
                })
                .await;

                this.update(cx, |this, cx| {
                    this.center_on_first_change_if_needed(cx);
                })?;
            }

            Ok(())
        });

        let refresh_history_task = cx.spawn(async move |this, cx| {
            while history_refresh_rx.recv().await.is_ok() {
                loop {
                    let mut timer = cx
                        .background_executor()
                        .timer(HISTORY_REFRESH_DEBOUNCE)
                        .fuse();
                    let mut recv = pin!(history_refresh_rx.recv().fuse());
                    select_biased! {
                        _ = timer => break,
                        _ = recv => continue,
                    }
                }

                this.update(cx, |this, cx| {
                    this.refresh_entries(cx);
                })?;
            }

            Ok(())
        });

        let mut this = Self {
            abs_path,
            current_buffer,
            snapshot_buffer,
            diff_editor,
            entries,
            selected_entry_id,
            entry_context_menu: None,
            scroll_handle: UniformListScrollHandle::new(),
            should_center_first_change: true,
            buffer_changes_tx,
            history_refresh_tx,
            _subscriptions: subscriptions,
            _recalculate_diff_task: recalculate_diff_task,
            _refresh_history_task: refresh_history_task,
        };

        this.notify_buffer_changes();
        this
    }

    fn notify_buffer_changes(&mut self) {
        if let Err(error) = self.buffer_changes_tx.send(()) {
            log::debug!("failed to notify local history buffer changes: {error}");
        }
    }

    fn notify_history_refresh(&mut self) {
        if let Err(error) = self.history_refresh_tx.send(()) {
            log::debug!("failed to notify local history refresh: {error}");
        }
    }

    fn sync_snapshot_language(&mut self, cx: &mut Context<Self>) {
        let language = self.current_buffer.read(cx).language().cloned();
        self.snapshot_buffer.update(cx, |buffer, cx| {
            buffer.set_language(language, cx);
            buffer.set_capability(Capability::ReadOnly, cx);
        });
    }

    fn update_snapshot_contents(&mut self, contents: String, cx: &mut Context<Self>) {
        self.sync_snapshot_language(cx);
        self.snapshot_buffer.update(cx, |buffer, cx| {
            buffer.set_text(contents, cx);
            buffer.set_capability(Capability::ReadOnly, cx);
        });
        self.should_center_first_change = true;
        self.notify_buffer_changes();
        cx.notify();
    }

    fn select_entry(&mut self, entry_id: i64, cx: &mut Context<Self>) {
        self.dismiss_entry_context_menu();

        if self.selected_entry_id == Some(entry_id) {
            return;
        }

        let contents = load_entry_contents(entry_id, cx);
        self.selected_entry_id = Some(entry_id);

        if let Some(contents) = contents {
            self.update_snapshot_contents(contents, cx);
        } else {
            cx.notify();
        }
    }

    fn refresh_entries(&mut self, cx: &mut Context<Self>) {
        self.dismiss_entry_context_menu();

        let Some(current_path) = current_buffer_path(&self.current_buffer, cx) else {
            return;
        };
        let current_text = self.current_buffer.read(cx).snapshot().text().to_string();

        let path_changed = self.abs_path != current_path;
        let follow_latest = self.selected_entry_id.is_none()
            || self.selected_entry_id == self.entries.first().map(|entry| entry.id);
        let entries = visible_entries(load_entries(&current_path, cx), &current_text, cx);
        let next_selected_id = if follow_latest {
            entries.first().map(|entry| entry.id)
        } else {
            self.selected_entry_id
                .filter(|selected_id| entries.iter().any(|entry| entry.id == *selected_id))
                .or_else(|| entries.first().map(|entry| entry.id))
        };

        self.abs_path = current_path;
        self.entries = entries;

        match next_selected_id {
            Some(entry_id) if self.selected_entry_id != Some(entry_id) || path_changed => {
                self.selected_entry_id = Some(entry_id);
                if let Some(contents) = load_entry_contents(entry_id, cx) {
                    self.update_snapshot_contents(contents, cx);
                } else {
                    cx.notify();
                }
            }
            None => {
                self.selected_entry_id = None;
                self.update_snapshot_contents(current_text, cx);
            }
            _ => cx.notify(),
        }
    }

    fn center_on_first_change_if_needed(&mut self, cx: &mut Context<Self>) {
        if !self.should_center_first_change {
            return;
        }

        self.should_center_first_change = false;
        self.diff_editor.update(cx, |splittable, cx| {
            splittable.rhs_editor().update(cx, |editor, cx| {
                let snapshot = editor.buffer().read(cx).snapshot(cx);
                if let Some(first_hunk) = snapshot.diff_hunks().next() {
                    editor.request_autoscroll(
                        Autoscroll::center().for_anchor(first_hunk.multi_buffer_range.start),
                        cx,
                    );
                }
            });
        });
    }

    fn dismiss_entry_context_menu(&mut self) {
        self.entry_context_menu = None;
    }

    fn show_entry_context_menu(
        &mut self,
        entry_id: i64,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.dismiss_entry_context_menu();

        let view = cx.entity();
        let context_menu = ContextMenu::build(window, cx, move |menu, _, _| {
            menu.entry("Revert to Selected", None, {
                let view = view.clone();
                move |_window, cx| {
                    view.update(cx, |this, cx| {
                        this.revert_to_entry(entry_id, cx);
                    });
                }
            })
        });

        window.focus(&context_menu.focus_handle(cx), cx);
        let subscription = cx.subscribe_in(
            &context_menu,
            window,
            |this, _, _: &DismissEvent, window, cx| {
                this.entry_context_menu = None;
                this.focus_handle(cx).focus(window, cx);
                cx.notify();
            },
        );
        self.entry_context_menu = Some((context_menu, subscription, entry_id));
        cx.notify();
    }

    fn revert_to_entry(&mut self, entry_id: i64, cx: &mut Context<Self>) {
        let Some(contents) = load_entry_contents(entry_id, cx) else {
            return;
        };

        self.dismiss_entry_context_menu();
        self.current_buffer.update(cx, |buffer, cx| {
            buffer.set_text(contents, cx);
        });
        self.refresh_entries(cx);
    }

    fn selected_entry(&self) -> Option<&LocalHistoryEntry> {
        self.selected_entry_id
            .and_then(|selected_id| self.entries.iter().find(|entry| entry.id == selected_id))
    }

    fn render_entry(&self, entry: &LocalHistoryEntry, cx: &mut Context<Self>) -> AnyElement {
        let entry_id = entry.id;
        let context_menu = self
            .entry_context_menu
            .as_ref()
            .filter(|(_, _, open_entry_id)| *open_entry_id == entry_id)
            .map(|(menu, _, _)| menu.clone());

        ListItem::new(("local-history-entry", entry_id as u64))
            .toggle_state(self.selected_entry_id == Some(entry_id))
            .inset(true)
            .child(
                v_flex()
                    .w_full()
                    .gap_0p5()
                    .child(
                        Label::new("Change")
                            .size(LabelSize::Small)
                            .color(Color::Default)
                            .truncate(),
                    )
                    .child(
                        Label::new(format_timestamp(entry.saved_at_unix_ms))
                            .size(LabelSize::XSmall)
                            .color(Color::Muted)
                            .truncate(),
                    ),
            )
            .end_slot(h_flex().children(context_menu.map(|menu| {
                deferred(
                    anchored()
                        .anchor(gpui::Corner::TopRight)
                        .child(menu.clone()),
                )
                .with_priority(1)
            })))
            .on_click(cx.listener(move |this, _, _, cx| {
                this.select_entry(entry_id, cx);
            }))
            .on_secondary_mouse_down(cx.listener(
                move |this, _event: &MouseDownEvent, window, cx| {
                    this.select_entry(entry_id, cx);
                    this.show_entry_context_menu(entry_id, window, cx);
                    cx.stop_propagation();
                },
            ))
            .into_any_element()
    }

    fn tab_title(&self) -> SharedString {
        let file_name = self
            .abs_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "File".to_string());
        format!("Local History: {}", file_name).into()
    }
    fn cancel(&mut self, _: &menu::Cancel, _: &mut Window, cx: &mut Context<Self>) {
        cx.emit(DismissEvent);
    }

    fn render_content(&mut self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let selected_label = self
            .selected_entry()
            .map(|entry| format!("Snapshot: {}", format_timestamp(entry.saved_at_unix_ms)))
            .unwrap_or_else(|| "Snapshot".to_string());
        let file_name = self
            .abs_path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "File".to_string());

        h_flex()
            .id("local-history-view")
            .size_full()
            .bg(cx.theme().colors().editor_background)
            .child(
                v_flex()
                    .w(px(SIDEBAR_WIDTH))
                    .h_full()
                    .border_r_1()
                    .border_color(cx.theme().colors().border_variant)
                    .child(
                        v_flex()
                            .gap_0p5()
                            .px_3()
                            .py_2()
                            .border_b_1()
                            .border_color(cx.theme().colors().border_variant)
                            .child(
                                Label::new("Local History")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(Label::new(file_name).color(Color::Default).truncate())
                            .child(
                                Label::new(format!("{} snapshots", self.entries.len()))
                                    .size(LabelSize::XSmall)
                                    .color(Color::Muted),
                            )
                            .child(
                                Label::new(self.abs_path.compact().to_string_lossy().into_owned())
                                    .size(LabelSize::XSmall)
                                    .color(Color::Muted)
                                    .truncate(),
                            ),
                    )
                    .child(if self.entries.is_empty() {
                        div()
                            .flex_1()
                            .items_center()
                            .justify_center()
                            .px_3()
                            .child(
                                v_flex()
                                    .gap_1()
                                    .items_center()
                                    .child(
                                        Label::new("No older snapshots yet")
                                            .size(LabelSize::Small)
                                            .color(Color::Muted),
                                    )
                                    .child(
                                        Label::new(
                                            "Save again to compare against an earlier version",
                                        )
                                        .size(LabelSize::XSmall)
                                        .color(Color::Muted),
                                    ),
                            )
                            .into_any_element()
                    } else {
                        let view = cx.weak_entity();
                        uniform_list(
                            "local-history-list",
                            self.entries.len(),
                            move |range, _, cx| {
                                let Some(view) = view.upgrade() else {
                                    return Vec::new();
                                };

                                view.update(cx, |this, cx| {
                                    range
                                        .filter_map(|ix| this.entries.get(ix))
                                        .map(|entry| this.render_entry(entry, cx))
                                        .collect()
                                })
                            },
                        )
                        .flex_1()
                        .size_full()
                        .track_scroll(&self.scroll_handle)
                        .into_any_element()
                    })
                    .vertical_scrollbar_for(&self.scroll_handle, window, cx),
            )
            .child(
                v_flex()
                    .flex_1()
                    .min_w_0()
                    .h_full()
                    .child(
                        h_flex()
                            .h(px(36.))
                            .px_3()
                            .justify_between()
                            .border_b_1()
                            .border_color(cx.theme().colors().border_variant)
                            .child(
                                Label::new(selected_label)
                                    .size(LabelSize::Small)
                                    .color(Color::Muted)
                                    .truncate(),
                            )
                            .child(
                                Label::new("Current")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            ),
                    )
                    .child(div().flex_1().min_h_0().child(self.diff_editor.clone())),
            )
            .into_any_element()
    }
}

impl EventEmitter<DismissEvent> for LocalHistoryModal {}

impl ModalView for LocalHistoryModal {}

impl Focusable for LocalHistoryModal {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.diff_editor.focus_handle(cx)
    }
}

impl Render for LocalHistoryModal {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .id("local-history-modal")
            .key_context("LocalHistoryModal")
            .on_action(cx.listener(Self::cancel))
            .elevation_3(cx)
            .bg(cx.theme().colors().elevated_surface_background)
            .border_1()
            .border_color(cx.theme().colors().border_variant)
            .rounded_lg()
            .overflow_hidden()
            .w(rems(92.))
            .max_w(relative(0.96))
            .h(vh(0.82, window))
            .max_h(vh(0.9, window))
            .child(
                h_flex()
                    .h(px(42.))
                    .px_3()
                    .justify_between()
                    .border_b_1()
                    .border_color(cx.theme().colors().border_variant)
                    .bg(cx.theme().colors().surface_background)
                    .child(
                        h_flex()
                            .gap_2()
                            .child(Icon::new(IconName::HistoryRerun).color(Color::Muted))
                            .child(
                                Label::new(self.tab_title())
                                    .color(Color::Default)
                                    .truncate(),
                            ),
                    )
                    .child(
                        Label::new(self.abs_path.compact().to_string_lossy().into_owned())
                            .size(LabelSize::Small)
                            .color(Color::Muted)
                            .truncate(),
                    ),
            )
            .child(self.render_content(window, cx))
    }
}

fn build_snapshot_buffer(
    current_buffer: &Entity<Buffer>,
    text: String,
    cx: &mut App,
) -> Entity<Buffer> {
    let language = current_buffer.read(cx).language().cloned();
    cx.new(|cx| {
        let mut buffer = Buffer::local(text, cx);
        buffer.set_language(language, cx);
        buffer.set_capability(Capability::ReadOnly, cx);
        buffer
    })
}

fn current_buffer_path(buffer: &Entity<Buffer>, cx: &App) -> Option<PathBuf> {
    let file = buffer.read(cx).file()?;
    if file.is_private() {
        return None;
    }

    Some(file.full_path(cx).to_path_buf())
}

fn load_entries(path: &Path, cx: &App) -> Vec<LocalHistoryEntry> {
    let abs_path = path.to_string_lossy().into_owned();
    EditorDb::global(cx)
        .get_local_history_entries(&abs_path, LOCAL_HISTORY_ENTRY_LIMIT)
        .log_err()
        .map(|entries| {
            entries
                .into_iter()
                .map(|(id, saved_at_unix_ms)| LocalHistoryEntry {
                    id,
                    saved_at_unix_ms,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn visible_entries(
    mut entries: Vec<LocalHistoryEntry>,
    current_text: &str,
    cx: &App,
) -> Vec<LocalHistoryEntry> {
    let should_hide_current_snapshot = entries
        .first()
        .and_then(|entry| load_entry_contents(entry.id, cx))
        .as_deref()
        == Some(current_text);

    if should_hide_current_snapshot {
        entries.remove(0);
    }

    entries
}

fn load_entry_contents(entry_id: i64, cx: &App) -> Option<String> {
    EditorDb::global(cx)
        .get_local_history_entry_contents(entry_id)
        .log_err()
        .flatten()
}

fn format_timestamp(saved_at_unix_ms: i64) -> String {
    let timestamp =
        OffsetDateTime::from_unix_timestamp_nanos(i128::from(saved_at_unix_ms) * 1_000_000)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let local_offset = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    let local_timestamp = timestamp.to_offset(local_offset);

    local_timestamp
        .format(format_description!(
            "[day]/[month]/[year], [hour]:[minute]:[second]"
        ))
        .unwrap_or_else(|_| local_timestamp.to_string())
}
