use agent_settings::AgentSettings;
use anyhow::{Context as _, Result, anyhow};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use db::kvp::KeyValueStore;
use editor::{Editor, EditorEvent, EditorMode, SizingBehavior};
use futures::TryStreamExt;
use gpui::{
    AnyElement, AnyView, App, AsyncWindowContext, ClickEvent, ClipboardItem, Context, DismissEvent,
    DragMoveEvent, Entity, EntityInputHandler, EventEmitter, FocusHandle, Focusable, MouseButton,
    MouseUpEvent, ParentElement, Pixels, Render, ScrollHandle, StatefulInteractiveElement,
    StyleRefinement, Subscription, Task, WeakEntity, Window, actions, canvas, div, px,
};
use gpui_tokio::Tokio;
use serde::{Deserialize, Serialize};
use settings::Settings;
use sqlx::{
    Column as _, Connection, Either, Executor as _, MySqlConnection, PgConnection, Row,
    TypeInfo as _, ValueRef as _, mysql::MySqlRow, postgres::PgRow, query, query_scalar, raw_sql,
};
use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};
use ui::{
    Button, ButtonSize, ButtonStyle, Clickable, Color, ContextMenu, ContextMenuEntry, Disclosure,
    Icon, IconButton, IconButtonShape, IconName, IconSize, Label, LabelSize, Modal, ModalFooter,
    ModalHeader, RedistributableColumnsState, Section, SectionHeader, TableResizeBehavior,
    TintColor, Tooltip, bind_redistributable_columns, h_flex, prelude::*,
    render_redistributable_columns_resize_handles, right_click_menu, v_flex,
};
use url::Url;
use uuid::Uuid;
use workspace::{
    Item, ItemHandle, ModalView, ToolbarItemEvent, ToolbarItemLocation, ToolbarItemView, Workspace,
    dock::{DockPosition, Panel, PanelEvent, PanelSizePersistence},
};

const DATABASE_PANEL_KEY: &str = "database_panel";
const DATABASE_CONNECTIONS_KEY: &str = "connections";
const QUERY_CONSOLE_MAX_ROWS: usize = 200;
const QUERY_CONSOLE_MAX_CELL_WIDTH: usize = 80;
const QUERY_CONSOLE_EMPTY_RESULTS: &str = "Run a query to see results here.";
const QUERY_CONSOLE_UI_STATE_KEY: &str = "query_console_ui_state";
const QUERY_CONSOLE_DEFAULT_RESULTS_PANEL_HEIGHT: Pixels = px(320.);
const QUERY_CONSOLE_MIN_RESULTS_PANEL_HEIGHT: Pixels = px(180.);
const QUERY_CONSOLE_MIN_SQL_PANEL_HEIGHT: Pixels = px(140.);
const QUERY_CONSOLE_RESULTS_RESIZE_HANDLE_SIZE: Pixels = px(6.);
const QUERY_CONSOLE_RESULT_COLUMN_MIN_WIDTH: Pixels = px(92.);
const QUERY_CONSOLE_RESULT_COLUMN_MAX_WIDTH: Pixels = px(240.);
const QUERY_CONSOLE_RESULT_COLUMN_HORIZONTAL_PADDING: Pixels = px(28.);
const QUERY_CONSOLE_RESULT_COLUMN_PIXELS_PER_CHARACTER: f32 = 7.;
const QUERY_CONSOLE_INLINE_ACTIONS_EXTRA_WIDTH: Pixels = px(140.);
const QUERY_CONSOLE_LAST_COLUMN_MIN_WIDTH_WITH_INLINE_ACTIONS: Pixels = px(220.);
const QUERY_CONSOLE_LAST_COLUMN_MAX_WIDTH_WITH_INLINE_ACTIONS: Pixels = px(320.);
const QUERY_CONSOLE_LAST_COLUMN_MIN_FRACTION: f32 = 0.18;

actions!(
    database_panel,
    [
        /// Toggles the database panel.
        Toggle,
        /// Toggles focus on the database panel.
        ToggleFocus
    ]
);

pub fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _window, _cx| {
        workspace.register_action(|workspace, _: &ToggleFocus, window, cx| {
            workspace.toggle_panel_focus::<DatabasePanel>(window, cx);
        });
        workspace.register_action(|workspace, _: &Toggle, window, cx| {
            if !workspace.toggle_panel_focus::<DatabasePanel>(window, cx) {
                workspace.close_panel::<DatabasePanel>(window, cx);
            }
        });
    })
    .detach();
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum DatabaseKind {
    Postgres,
    MySql,
}

impl DatabaseKind {
    fn from_scheme(scheme: &str) -> Option<Self> {
        match scheme {
            "postgres" | "postgresql" => Some(Self::Postgres),
            "mysql" | "mariadb" => Some(Self::MySql),
            _ => None,
        }
    }

    fn normalized_scheme(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Postgres => "Postgres",
            Self::MySql => "MySQL",
        }
    }

    fn default_port(self) -> u16 {
        match self {
            Self::Postgres => 5432,
            Self::MySql => 3306,
        }
    }

    fn schema_label(self) -> &'static str {
        match self {
            Self::Postgres => "schema",
            Self::MySql => "database",
        }
    }

    fn supports_table_ddl(self) -> bool {
        matches!(self, Self::MySql)
    }

    fn supports_table_editor(self) -> bool {
        matches!(self, Self::MySql)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SavedDatabaseConnection {
    id: Uuid,
    name: String,
    url: String,
    kind: DatabaseKind,
}

#[derive(Clone, Debug)]
struct DatabaseMetadata {
    schemas: Vec<DatabaseSchema>,
}

#[derive(Clone, Debug)]
struct DatabaseSchema {
    name: String,
    tables: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct DatabaseTablePath {
    schema_name: String,
    table_name: String,
}

impl DatabaseTablePath {
    fn new(schema_name: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            schema_name: schema_name.into(),
            table_name: table_name.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum DatabaseTableSectionKind {
    Columns,
    Keys,
    Indexes,
}

impl DatabaseTableSectionKind {
    fn label(self) -> &'static str {
        match self {
            Self::Columns => "columns",
            Self::Keys => "keys",
            Self::Indexes => "indexes",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct DatabaseTableSectionPath {
    table_path: DatabaseTablePath,
    section_kind: DatabaseTableSectionKind,
}

impl DatabaseTableSectionPath {
    fn new(table_path: DatabaseTablePath, section_kind: DatabaseTableSectionKind) -> Self {
        Self {
            table_path,
            section_kind,
        }
    }
}

#[derive(Debug)]
enum ConnectionLoadState {
    Idle,
    Loading,
    Loaded(DatabaseMetadata),
    Failed(String),
}

struct DatabaseConnectionEntry {
    saved: SavedDatabaseConnection,
    expanded: bool,
    expanded_schemas: HashSet<String>,
    expanded_tables: HashSet<DatabaseTablePath>,
    expanded_table_sections: HashSet<DatabaseTableSectionPath>,
    table_browser_load_states: HashMap<DatabaseTablePath, TableBrowserLoadState>,
    load_state: ConnectionLoadState,
}

impl DatabaseConnectionEntry {
    fn new(saved: SavedDatabaseConnection) -> Self {
        Self {
            saved,
            expanded: false,
            expanded_schemas: HashSet::default(),
            expanded_tables: HashSet::default(),
            expanded_table_sections: HashSet::default(),
            table_browser_load_states: HashMap::default(),
            load_state: ConnectionLoadState::Idle,
        }
    }
}

#[derive(Clone, Copy)]
enum NoticeKind {
    Error,
    Info,
    Success,
}

struct PanelNotice {
    kind: NoticeKind,
    message: String,
}

#[derive(Clone, Debug)]
struct DatabaseTableColumn {
    name: String,
    detail: String,
}

#[derive(Clone, Debug)]
struct DatabaseTableKey {
    name: String,
    detail: String,
    constraint_type: String,
    column_names: Vec<String>,
}

#[derive(Clone, Debug)]
struct DatabaseTableIndex {
    name: String,
    detail: String,
}

#[derive(Clone, Debug)]
struct DatabaseTableBrowserDetails {
    columns: Vec<DatabaseTableColumn>,
    keys: Vec<DatabaseTableKey>,
    indexes: Vec<DatabaseTableIndex>,
}

#[derive(Debug)]
enum TableBrowserLoadState {
    Loading,
    Loaded(DatabaseTableBrowserDetails),
    Failed(String),
}

#[derive(Clone, Debug)]
struct DatabaseTableDetails {
    kind: DatabaseKind,
    schema_name: String,
    table_name: String,
    engine: String,
    collation: String,
    comment: String,
    create_options: String,
    create_table_ddl: String,
    columns: Vec<DatabaseTableColumn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TableEditorStatus {
    Loading,
    Ready,
    Saving,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TableEditorValues {
    table_name: String,
    engine: String,
    collation: String,
    comment: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MySqlTableEditPlan {
    rename_statement: Option<String>,
    alter_statement: Option<String>,
    target_schema_name: String,
    target_table_name: String,
}

impl MySqlTableEditPlan {
    fn is_empty(&self) -> bool {
        self.rename_statement.is_none() && self.alter_statement.is_none()
    }

    fn preview(&self) -> String {
        let mut statements = Vec::new();
        if let Some(rename_statement) = &self.rename_statement {
            statements.push(format!("{rename_statement};"));
        }
        if let Some(alter_statement) = &self.alter_statement {
            statements.push(format!("{alter_statement};"));
        }
        statements.join("\n\n")
    }
}

struct DatabaseConnectionForm {
    kind: DatabaseKind,
    host: String,
    port: String,
    user: String,
    password: String,
    database_name: String,
    options: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryConsoleStatus {
    Idle,
    Running,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryConsoleRunTarget {
    SelectionOrCurrent,
    All,
}

#[derive(Clone, Debug)]
struct ParsedQueryStatement {
    sql: String,
    start_utf16: usize,
    end_utf16: usize,
}

#[derive(Clone, Debug)]
struct QueryStatementExecution {
    statement: String,
    column_names: Vec<String>,
    rows: Vec<QueryConsoleResultRow>,
    rows_affected: u64,
    row_count: usize,
    truncated: bool,
}

#[derive(Clone, Debug)]
enum QueryConsoleSqlValue {
    Null,
    Number(String),
    Text(String),
    Bytes(Vec<u8>),
}

#[derive(Clone, Debug)]
struct QueryConsoleCellValue {
    display_text: String,
    sql_value: QueryConsoleSqlValue,
}

#[derive(Clone, Debug)]
struct QueryConsoleResultRow {
    cells: Vec<QueryConsoleCellValue>,
}

#[derive(Clone, Debug)]
struct QueryConsoleTableMutationMetadata {
    table_path: DatabaseTablePath,
    table_column_names: Vec<String>,
    key_column_names: Vec<String>,
}

#[derive(Clone, Debug)]
struct QueryConsoleResultTable {
    statement: String,
    column_names: Vec<String>,
    rows: Vec<QueryConsoleResultRow>,
    row_count: usize,
    truncated: bool,
    mutation_metadata: Option<QueryConsoleTableMutationMetadata>,
    mutation_message: Option<String>,
}

#[derive(Clone, Debug)]
struct QueryConsoleExecutionSummary {
    output_text: String,
    completed_statement_count: usize,
    stopped_at_statement_number: Option<usize>,
    error_message: Option<String>,
    result_table: Option<QueryConsoleResultTable>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SerializedQueryConsoleUiState {
    #[serde(default = "default_query_console_results_panel_height_value")]
    results_panel_height: f32,
    #[serde(default)]
    summary_expanded: bool,
}

impl Default for SerializedQueryConsoleUiState {
    fn default() -> Self {
        Self {
            results_panel_height: default_query_console_results_panel_height_value(),
            summary_expanded: false,
        }
    }
}

#[derive(Clone)]
struct DraggedQueryConsoleResultsResizeHandle;

impl Render for DraggedQueryConsoleResultsResizeHandle {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        gpui::Empty
    }
}

pub struct DatabasePanel {
    focus_handle: FocusHandle,
    connection_kind: DatabaseKind,
    database_editor: Entity<Editor>,
    name_editor: Entity<Editor>,
    notice: Option<PanelNotice>,
    host_editor: Entity<Editor>,
    connections: Vec<DatabaseConnectionEntry>,
    editing_connection_id: Option<Uuid>,
    options_editor: Entity<Editor>,
    password_editor: Entity<Editor>,
    port_editor: Entity<Editor>,
    show_add_connection_form: bool,
    user_editor: Entity<Editor>,
    workspace: WeakEntity<Workspace>,
    zoomed: bool,
}

impl DatabasePanel {
    fn new(
        workspace: WeakEntity<Workspace>,
        saved_connections: Vec<SavedDatabaseConnection>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let default_kind = DatabaseKind::Postgres;
        let name_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Production analytics", window, cx);
            editor
        });
        let host_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("db.example.com", window, cx);
            editor
        });
        let port_placeholder = default_kind.default_port().to_string();
        let port_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text(&port_placeholder, window, cx);
            editor
        });
        let user_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("readonly_user", window, cx);
            editor
        });
        let password_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Password", window, cx);
            editor.set_masked(true, cx);
            editor
        });
        let database_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("main_db", window, cx);
            editor
        });
        let options_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("sslmode=require", window, cx);
            editor
        });

        Self {
            focus_handle: cx.focus_handle(),
            connection_kind: default_kind,
            database_editor,
            name_editor,
            notice: None,
            host_editor,
            show_add_connection_form: saved_connections.is_empty(),
            connections: saved_connections
                .into_iter()
                .map(DatabaseConnectionEntry::new)
                .collect(),
            editing_connection_id: None,
            options_editor,
            password_editor,
            port_editor,
            user_editor,
            workspace,
            zoomed: false,
        }
    }

    fn clear_add_connection_inputs(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.name_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.host_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.port_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.user_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.password_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.database_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.options_editor.update(cx, |editor, cx| {
            editor.set_text("", window, cx);
        });
        self.update_port_placeholder(window, cx);
    }

    fn delete_connection(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.connections
            .retain(|connection| connection.saved.id != connection_id);
        if self.editing_connection_id == Some(connection_id) {
            self.editing_connection_id = None;
            self.show_add_connection_form = false;
            self.clear_add_connection_inputs(window, cx);
        }
        if self.connections.is_empty() {
            self.show_add_connection_form = true;
        }
        self.notice = Some(PanelNotice {
            kind: NoticeKind::Info,
            message: "Connection removed.".to_string(),
        });
        self.persist_connections(cx);
        cx.notify();
    }

    fn persist_connections(&self, cx: &App) {
        let connections = self
            .connections
            .iter()
            .map(|connection| connection.saved.clone())
            .collect::<Vec<_>>();
        Self::persist_saved_connections(connections, cx);
    }

    fn persist_saved_connections(connections: Vec<SavedDatabaseConnection>, cx: &App) {
        let kvp = KeyValueStore::global(cx);
        db::write_and_log(cx, move || async move {
            let json = serde_json::to_string(&connections)?;
            kvp.scoped(DATABASE_PANEL_KEY)
                .write(DATABASE_CONNECTIONS_KEY.to_string(), json)
                .await
        });
    }

    fn close_connection_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.show_add_connection_form = false;
        self.editing_connection_id = None;
        self.clear_add_connection_inputs(window, cx);
    }

    fn open_new_connection_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.show_add_connection_form = true;
        self.editing_connection_id = None;
        self.clear_add_connection_inputs(window, cx);
        self.name_editor.focus_handle(cx).focus(window, cx);
        cx.notify();
    }

    fn set_connection_kind(
        &mut self,
        kind: DatabaseKind,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.connection_kind = kind;
        self.update_port_placeholder(window, cx);
        cx.notify();
    }

    fn update_port_placeholder(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let port_placeholder = self.connection_kind.default_port().to_string();
        self.port_editor.update(cx, |editor, cx| {
            editor.set_placeholder_text(&port_placeholder, window, cx);
        });
    }

    fn read_connection_form(&self, cx: &App) -> DatabaseConnectionForm {
        DatabaseConnectionForm {
            kind: self.connection_kind,
            host: self.host_editor.read(cx).text(cx).trim().to_string(),
            port: self.port_editor.read(cx).text(cx).trim().to_string(),
            user: self.user_editor.read(cx).text(cx).trim().to_string(),
            password: self.password_editor.read(cx).text(cx).to_string(),
            database_name: self.database_editor.read(cx).text(cx).trim().to_string(),
            options: self.options_editor.read(cx).text(cx).trim().to_string(),
        }
    }

    fn set_connection_form(
        &mut self,
        form: DatabaseConnectionForm,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.connection_kind = form.kind;
        self.update_port_placeholder(window, cx);
        self.host_editor.update(cx, |editor, cx| {
            editor.set_text(form.host.as_str(), window, cx);
        });
        self.port_editor.update(cx, |editor, cx| {
            editor.set_text(form.port.as_str(), window, cx);
        });
        self.user_editor.update(cx, |editor, cx| {
            editor.set_text(form.user.as_str(), window, cx);
        });
        self.password_editor.update(cx, |editor, cx| {
            editor.set_text(form.password.as_str(), window, cx);
        });
        self.database_editor.update(cx, |editor, cx| {
            editor.set_text(form.database_name.as_str(), window, cx);
        });
        self.options_editor.update(cx, |editor, cx| {
            editor.set_text(form.options.as_str(), window, cx);
        });
    }

    fn edit_connection(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(saved_connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
            .map(|connection| connection.saved.clone())
        else {
            return;
        };

        let connection_form = match connection_form_from_saved_connection(&saved_connection) {
            Ok(connection_form) => connection_form,
            Err(error) => {
                self.notice = Some(PanelNotice {
                    kind: NoticeKind::Error,
                    message: error.to_string(),
                });
                cx.notify();
                return;
            }
        };

        self.show_add_connection_form = true;
        self.editing_connection_id = Some(connection_id);
        self.name_editor.update(cx, |editor, cx| {
            editor.set_text(saved_connection.name.as_str(), window, cx);
        });
        self.set_connection_form(connection_form, window, cx);
        self.name_editor.focus_handle(cx).focus(window, cx);
        cx.notify();
    }

    fn read_saved_connections(cx: &App) -> Vec<SavedDatabaseConnection> {
        let kvp = KeyValueStore::global(cx);
        let scope = kvp.scoped(DATABASE_PANEL_KEY);
        let json = match scope.read(DATABASE_CONNECTIONS_KEY) {
            Ok(value) => value,
            Err(error) => {
                log::error!("Failed to read saved database connections: {error:#}");
                return Vec::new();
            }
        };

        match json {
            Some(json) => match serde_json::from_str::<Vec<SavedDatabaseConnection>>(&json) {
                Ok(connections) => {
                    let mut normalized_any_connection = false;
                    let normalized_connections = connections
                        .into_iter()
                        .map(|connection| {
                            let original_connection = connection.clone();
                            match normalize_saved_connection(connection) {
                                Ok(connection) => {
                                    if connection.url != original_connection.url
                                        || connection.kind != original_connection.kind
                                    {
                                        normalized_any_connection = true;
                                    }
                                    connection
                                }
                                Err(error) => {
                                    log::error!(
                                        "Failed to normalize saved database connection: {error:#}"
                                    );
                                    original_connection
                                }
                            }
                        })
                        .collect::<Vec<_>>();

                    if normalized_any_connection {
                        Self::persist_saved_connections(normalized_connections.clone(), cx);
                    }

                    normalized_connections
                }
                Err(error) => {
                    log::error!("Failed to parse saved database connections: {error:#}");
                    Vec::new()
                }
            },
            None => Vec::new(),
        }
    }

    fn refresh_all_connections(&mut self, cx: &mut Context<Self>) {
        let connection_ids = self
            .connections
            .iter()
            .map(|connection| connection.saved.id)
            .collect::<Vec<_>>();
        for connection_id in connection_ids {
            self.refresh_connection(connection_id, cx);
        }
    }

    fn refresh_connection(&mut self, connection_id: Uuid, cx: &mut Context<Self>) {
        let Some(connection) = self
            .connections
            .iter_mut()
            .find(|connection| connection.saved.id == connection_id)
        else {
            return;
        };

        let connection_kind = connection.saved.kind;
        let connection_name = connection.saved.name.clone();
        let connection_url = connection.saved.url.clone();
        let connection_url_for_task = connection_url.clone();
        connection.expanded = true;
        connection.expanded_tables.clear();
        connection.expanded_table_sections.clear();
        connection.table_browser_load_states.clear();
        connection.load_state = ConnectionLoadState::Loading;
        self.notice = None;
        cx.notify();

        cx.spawn(async move |this, cx| -> Result<()> {
            match Tokio::spawn_result(cx, async move {
                load_connection_metadata(connection_kind, connection_url_for_task).await
            })
            .await
            {
                Ok(metadata) => {
                    this.update(cx, |this, cx| {
                        if let Some(connection) = this
                            .connections
                            .iter_mut()
                            .find(|connection| connection.saved.id == connection_id)
                        {
                            connection.expanded_schemas =
                                default_expanded_schemas(connection.saved.kind, &metadata);
                            connection.expanded_tables.clear();
                            connection.expanded_table_sections.clear();
                            connection.table_browser_load_states.clear();
                            connection.load_state = ConnectionLoadState::Loaded(metadata);
                            this.notice = None;
                            cx.notify();
                        }
                    })?;
                }
                Err(error) => {
                    let error_message = format_connection_error(&error, &connection_url);
                    log::error!(
                        "Failed to connect to database connection {}: {}",
                        connection_name,
                        error_message
                    );
                    this.update(cx, |this, cx| {
                        if let Some(connection) = this
                            .connections
                            .iter_mut()
                            .find(|connection| connection.saved.id == connection_id)
                        {
                            connection.load_state =
                                ConnectionLoadState::Failed(error_message.clone());
                            this.notice = Some(PanelNotice {
                                kind: NoticeKind::Error,
                                message: format!(
                                    "Failed to connect to {}. {}",
                                    connection_name, error_message
                                ),
                            });
                            cx.notify();
                        }
                    })?;
                }
            }

            Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn render_add_connection_form(
        &self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let is_editing = self.editing_connection_id.is_some();
        let title = if is_editing {
            "Edit connection"
        } else {
            "New connection"
        };
        let submit_label = if is_editing {
            "Save Changes"
        } else {
            "Save & Connect"
        };

        v_flex()
            .w_full()
            .gap_3()
            .p_3()
            .rounded_md()
            .border_1()
            .border_color(cx.theme().colors().border_variant)
            .bg(cx.theme().colors().panel_background)
            .child(
                v_flex().gap_1().child(Label::new(title)).child(
                    Label::new("Fill in the connection details below. Passwords stay masked in the form.")
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                ),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        Label::new("Database")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .child(
                                Button::new("database-kind-postgres", "Postgres")
                                    .style(ButtonStyle::Subtle)
                                    .toggle_state(self.connection_kind == DatabaseKind::Postgres)
                                    .selected_style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.set_connection_kind(DatabaseKind::Postgres, window, cx);
                                    })),
                            )
                            .child(
                                Button::new("database-kind-mysql", "MySQL")
                                    .style(ButtonStyle::Subtle)
                                    .toggle_state(self.connection_kind == DatabaseKind::MySql)
                                    .selected_style(ButtonStyle::Filled)
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        this.set_connection_kind(DatabaseKind::MySql, window, cx);
                                    })),
                            ),
                    ),
            )
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        Label::new("Name")
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(render_database_input(self.name_editor.clone(), cx)),
            )
            .child(
                h_flex()
                    .w_full()
                    .gap_2()
                    .child(
                        v_flex()
                            .flex_1()
                            .gap_1()
                            .child(
                                Label::new("Host")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(render_database_input(self.host_editor.clone(), cx)),
                    )
                    .child(
                        v_flex()
                            .w(px(96.))
                            .gap_1()
                            .child(
                                Label::new("Port")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(render_database_input(self.port_editor.clone(), cx)),
                    ),
            )
            .child(render_database_field("User", self.user_editor.clone(), cx))
            .child(render_database_field(
                "Password",
                self.password_editor.clone(),
                cx,
            ))
            .child(
                h_flex()
                    .w_full()
                    .gap_2()
                    .child(
                        v_flex()
                            .flex_1()
                            .gap_1()
                            .child(
                                Label::new("Database Name (Optional)")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(render_database_input(self.database_editor.clone(), cx)),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .gap_1()
                            .child(
                                Label::new("Options (Optional)")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(render_database_input(self.options_editor.clone(), cx)),
                    ),
            )
            .child(
                Label::new("Connection details, including passwords, are stored locally in Zed's app database.")
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
            .child(
                h_flex()
                    .justify_end()
                    .gap_2()
                    .child(
                        Button::new("cancel-database-connection", "Cancel")
                            .style(ButtonStyle::Subtle)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.close_connection_form(window, cx);
                                cx.notify();
                            })),
                    )
                    .child(
                        Button::new("save-database-connection", submit_label)
                            .style(ButtonStyle::Filled)
                            .on_click(cx.listener(|this, _, window, cx| {
                                this.submit_connection_form(window, cx);
                            })),
                    ),
            )
    }

    fn render_connection(
        &self,
        connection: &DatabaseConnectionEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let panel = cx.entity();
        let connection_id = connection.saved.id;
        let disclosure_id = format!("database-connection-toggle-{connection_id}");
        let context_menu_id = format!("database-connection-context-menu-{connection_id}");
        let row_id = format!("database-connection-row-{connection_id}");
        let delete_id = format!("database-connection-delete-{connection_id}");
        let edit_id = format!("database-connection-edit-{connection_id}");
        let connect_button_id = format!("database-connection-connect-{connection_id}");
        let refresh_button_id = format!("database-connection-refresh-{connection_id}");
        let subtitle = connection_subtitle(&connection.saved);
        let action_label = match &connection.load_state {
            ConnectionLoadState::Failed(_) => Some("Retry"),
            ConnectionLoadState::Idle => Some("Connect"),
            ConnectionLoadState::Loading => None,
            ConnectionLoadState::Loaded(_) => None,
        };
        let table_count = match &connection.load_state {
            ConnectionLoadState::Loaded(metadata) => Some(total_table_count(metadata)),
            _ => None,
        };
        let connection_status = connection_load_state_status(&connection.load_state);
        let header = h_flex()
            .w_full()
            .items_start()
            .gap_1()
            .child(
                Disclosure::new(disclosure_id, connection.expanded).on_click(cx.listener(
                    move |this, _, _, cx| {
                        this.toggle_connection_expanded(connection_id, cx);
                    },
                )),
            )
            .child(
                h_flex()
                    .id(row_id)
                    .flex_1()
                    .min_w_0()
                    .px_2()
                    .py_1p5()
                    .gap_2()
                    .rounded_md()
                    .hover(|style| style.bg(cx.theme().colors().element_hover))
                    .cursor_pointer()
                    .on_click(cx.listener(move |this, _, _, cx| {
                        this.toggle_connection_expanded(connection_id, cx);
                    }))
                    .child(
                        Icon::new(IconName::Server)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .min_w_0()
                            .gap_0p5()
                            .child(
                                h_flex()
                                    .items_center()
                                    .gap_2()
                                    .child(Label::new(connection.saved.name.clone()))
                                    .when_some(connection_status, |this, (status_label, color)| {
                                        this.child(
                                            Label::new(status_label)
                                                .size(LabelSize::Small)
                                                .color(color),
                                        )
                                    }),
                            )
                            .child(
                                Label::new(subtitle)
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            ),
                    )
                    .when_some(table_count, |this, table_count| {
                        this.child(
                            Label::new(format!("{table_count} tables"))
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                    }),
            )
            .when_some(action_label, |this, action_label| {
                this.child(
                    Button::new(connect_button_id, action_label)
                        .size(ButtonSize::Compact)
                        .style(ButtonStyle::Subtle)
                        .on_click(cx.listener(move |this, _, _, cx| {
                            this.refresh_connection(connection_id, cx);
                        })),
                )
            })
            .when(
                matches!(&connection.load_state, ConnectionLoadState::Loaded(_)),
                |this| {
                    this.child(
                        IconButton::new(refresh_button_id, IconName::RotateCw)
                            .shape(IconButtonShape::Square)
                            .icon_size(IconSize::Small)
                            .tooltip(Tooltip::text("Refresh database metadata"))
                            .on_click(cx.listener(move |this, _, _, cx| {
                                this.refresh_connection(connection_id, cx);
                            })),
                    )
                },
            )
            .child(
                IconButton::new(edit_id, IconName::Pencil)
                    .shape(IconButtonShape::Square)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Edit connection"))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.edit_connection(connection_id, window, cx);
                    })),
            )
            .child(
                IconButton::new(delete_id, IconName::Trash)
                    .shape(IconButtonShape::Square)
                    .icon_size(IconSize::Small)
                    .tooltip(Tooltip::text("Remove connection"))
                    .on_click(cx.listener(move |this, _, window, cx| {
                        this.delete_connection(connection_id, window, cx);
                    })),
            );

        v_flex()
            .w_full()
            .gap_1()
            .child(
                right_click_menu(context_menu_id)
                    .trigger(move |_, _, _| header)
                    .menu(move |window, cx| {
                        panel.update(cx, |this, cx| {
                            this.build_connection_context_menu(connection_id, window, cx)
                        })
                    }),
            )
            .when(connection.expanded, |this| {
                this.child(self.render_connection_body(connection, cx))
            })
            .into_any_element()
    }

    fn render_connection_body(
        &self,
        connection: &DatabaseConnectionEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        match &connection.load_state {
            ConnectionLoadState::Idle => v_flex()
                .pl_8()
                .pb_2()
                .child(
                    Label::new("Click Connect to load schemas and tables.")
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
                .into_any_element(),
            ConnectionLoadState::Loading => v_flex()
                .pl_8()
                .pb_2()
                .child(
                    Label::new("Loading database metadata…")
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
                .into_any_element(),
            ConnectionLoadState::Failed(error) => v_flex()
                .pl_8()
                .pb_2()
                .gap_1()
                .child(
                    Label::new("Connection failed")
                        .size(LabelSize::Small)
                        .color(Color::Error),
                )
                .child(
                    Label::new(error.clone())
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
                .into_any_element(),
            ConnectionLoadState::Loaded(metadata) => {
                if metadata.schemas.is_empty() {
                    return v_flex()
                        .pl_8()
                        .pb_2()
                        .child(
                            Label::new("No schemas were returned for this connection.")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                        .into_any_element();
                }

                v_flex()
                    .w_full()
                    .gap_1()
                    .pl_8()
                    .children(metadata.schemas.iter().map(|schema| {
                        self.render_schema(connection.saved.id, schema, connection, cx)
                    }))
                    .into_any_element()
            }
        }
    }

    fn render_empty_state(&self, _cx: &mut Context<Self>) -> impl IntoElement {
        v_flex()
            .flex_1()
            .items_center()
            .justify_center()
            .gap_3()
            .px_6()
            .child(
                Icon::new(IconName::DatabaseZap)
                    .size(IconSize::Custom(rems_from_px(28.)))
                    .color(Color::Muted),
            )
            .child(Label::new("No database connections yet"))
            .child(
                Label::new(
                    "Add Postgres or MySQL/MariaDB connection details to browse schemas and tables.",
                )
                    .size(LabelSize::Small)
                    .color(Color::Muted),
            )
    }

    fn render_notice(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let notice = self.notice.as_ref()?;
        let color = match notice.kind {
            NoticeKind::Error => Color::Error,
            NoticeKind::Info => Color::Info,
            NoticeKind::Success => Color::Success,
        };

        Some(
            h_flex()
                .w_full()
                .px_3()
                .py_2()
                .rounded_md()
                .border_1()
                .border_color(color.color(cx).opacity(0.35))
                .bg(color.color(cx).opacity(0.08))
                .child(
                    Label::new(notice.message.clone())
                        .size(LabelSize::Small)
                        .color(color),
                )
                .into_any_element(),
        )
    }

    fn render_schema(
        &self,
        connection_id: Uuid,
        schema: &DatabaseSchema,
        connection: &DatabaseConnectionEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let panel = cx.entity();
        let schema_name = schema.name.clone();
        let schema_toggle_id = format!("database-schema-toggle-{connection_id}-{schema_name}");
        let context_menu_id = format!("database-schema-context-menu-{connection_id}-{schema_name}");
        let schema_row_id = format!("database-schema-row-{connection_id}-{schema_name}");
        let is_expanded = connection.expanded_schemas.contains(&schema.name);
        let row = h_flex()
            .w_full()
            .items_center()
            .gap_1()
            .child(
                Disclosure::new(schema_toggle_id, is_expanded).on_click(cx.listener({
                    let schema_name = schema.name.clone();
                    move |this, _, _, cx| {
                        this.toggle_schema_expanded(connection_id, schema_name.clone(), cx);
                    }
                })),
            )
            .child(
                h_flex()
                    .id(schema_row_id)
                    .flex_1()
                    .min_w_0()
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .hover(|style| style.bg(cx.theme().colors().element_hover))
                    .cursor_pointer()
                    .on_click(cx.listener({
                        let schema_name = schema.name.clone();
                        move |this, _, _, cx| {
                            this.toggle_schema_expanded(connection_id, schema_name.clone(), cx);
                        }
                    }))
                    .child(
                        Icon::new(IconName::Folder)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        Label::new(schema.name.clone())
                            .size(LabelSize::Small)
                            .color(Color::Default),
                    )
                    .child(
                        Label::new(format!("{} tables", schema.tables.len()))
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    ),
            );

        v_flex()
            .w_full()
            .gap_1()
            .child(
                right_click_menu(context_menu_id)
                    .trigger(move |_, _, _| row)
                    .menu({
                        let schema_name = schema.name.clone();
                        move |window, cx| {
                            panel.update(cx, |this, cx| {
                                this.build_schema_context_menu(
                                    connection_id,
                                    schema_name.clone(),
                                    window,
                                    cx,
                                )
                            })
                        }
                    }),
            )
            .when(is_expanded, |this| {
                this.child(
                    v_flex()
                        .w_full()
                        .gap_1()
                        .pl_8()
                        .children(schema.tables.iter().map(|table| {
                            self.render_table(connection_id, &schema.name, table, connection, cx)
                        })),
                )
            })
            .into_any_element()
    }

    fn render_table(
        &self,
        connection_id: Uuid,
        schema_name: &str,
        table_name: &str,
        connection: &DatabaseConnectionEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let panel = cx.entity();
        let table_path = DatabaseTablePath::new(schema_name, table_name);
        let context_menu_id =
            format!("database-table-context-menu-{connection_id}-{schema_name}-{table_name}");
        let table_toggle_id =
            format!("database-table-toggle-{connection_id}-{schema_name}-{table_name}");
        let table_row_id = format!("database-table-row-{connection_id}-{schema_name}-{table_name}");
        let is_expanded = connection.expanded_tables.contains(&table_path);
        let row = h_flex()
            .w_full()
            .items_center()
            .gap_1()
            .child(
                Disclosure::new(table_toggle_id, is_expanded).on_click(cx.listener({
                    let table_path = table_path.clone();
                    move |this, _, _, cx| {
                        this.toggle_table_expanded(connection_id, table_path.clone(), cx);
                    }
                })),
            )
            .child(
                h_flex()
                    .id(table_row_id)
                    .flex_1()
                    .min_w_0()
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .hover(|style| style.bg(cx.theme().colors().element_hover))
                    .cursor_pointer()
                    .on_click(cx.listener({
                        let table_path = table_path.clone();
                        move |this, _, _, cx| {
                            this.toggle_table_expanded(connection_id, table_path.clone(), cx);
                        }
                    }))
                    .child(
                        Icon::new(IconName::File)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        Label::new(table_name.to_string())
                            .size(LabelSize::Small)
                            .color(Color::Default),
                    ),
            );

        v_flex()
            .w_full()
            .gap_1()
            .child(
                right_click_menu(context_menu_id)
                    .trigger(move |_, _, _| row)
                    .menu({
                        let schema_name = schema_name.to_string();
                        let table_name = table_name.to_string();
                        move |window, cx| {
                            panel.update(cx, |this, cx| {
                                this.build_table_context_menu(
                                    connection_id,
                                    schema_name.clone(),
                                    table_name.clone(),
                                    window,
                                    cx,
                                )
                            })
                        }
                    }),
            )
            .when(is_expanded, |this| {
                let content = match connection.table_browser_load_states.get(&table_path) {
                    Some(TableBrowserLoadState::Loaded(details)) => self
                        .render_table_browser_details(
                            connection_id,
                            &table_path,
                            details,
                            connection,
                            cx,
                        ),
                    Some(TableBrowserLoadState::Failed(error)) => v_flex()
                        .w_full()
                        .pl_8()
                        .child(
                            Label::new(error.clone())
                                .size(LabelSize::Small)
                                .color(Color::Error),
                        )
                        .into_any_element(),
                    Some(TableBrowserLoadState::Loading) | None => v_flex()
                        .w_full()
                        .pl_8()
                        .child(
                            Label::new("Loading table structure…")
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                        .into_any_element(),
                };
                this.child(content)
            })
            .into_any_element()
    }

    fn render_table_browser_details(
        &self,
        connection_id: Uuid,
        table_path: &DatabaseTablePath,
        details: &DatabaseTableBrowserDetails,
        connection: &DatabaseConnectionEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let column_items = details
            .columns
            .iter()
            .map(|column| {
                self.render_table_browser_item(
                    IconName::File,
                    &column.name,
                    &column.detail,
                    Color::Default,
                    cx,
                )
            })
            .collect::<Vec<_>>();
        let key_items = details
            .keys
            .iter()
            .map(|key| {
                self.render_table_browser_item(
                    IconName::Link,
                    &key.name,
                    &key.detail,
                    Color::Default,
                    cx,
                )
            })
            .collect::<Vec<_>>();
        let index_items = details
            .indexes
            .iter()
            .map(|index| {
                self.render_table_browser_item(
                    IconName::Hash,
                    &index.name,
                    &index.detail,
                    Color::Default,
                    cx,
                )
            })
            .collect::<Vec<_>>();

        v_flex()
            .w_full()
            .gap_1()
            .pl_8()
            .child(self.render_table_browser_section(
                connection_id,
                table_path,
                DatabaseTableSectionKind::Columns,
                details.columns.len(),
                column_items,
                "No columns were found for this table.",
                connection,
                cx,
            ))
            .child(self.render_table_browser_section(
                connection_id,
                table_path,
                DatabaseTableSectionKind::Keys,
                details.keys.len(),
                key_items,
                "No keys were found for this table.",
                connection,
                cx,
            ))
            .child(self.render_table_browser_section(
                connection_id,
                table_path,
                DatabaseTableSectionKind::Indexes,
                details.indexes.len(),
                index_items,
                "No indexes were found for this table.",
                connection,
                cx,
            ))
            .into_any_element()
    }

    fn render_table_browser_section(
        &self,
        connection_id: Uuid,
        table_path: &DatabaseTablePath,
        section_kind: DatabaseTableSectionKind,
        item_count: usize,
        items: Vec<AnyElement>,
        empty_message: &'static str,
        connection: &DatabaseConnectionEntry,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let section_path = DatabaseTableSectionPath::new(table_path.clone(), section_kind);
        let section_name = section_kind.label();
        let section_toggle_id = format!(
            "database-table-section-toggle-{connection_id}-{}-{}-{section_name}",
            table_path.schema_name, table_path.table_name
        );
        let section_row_id = format!(
            "database-table-section-row-{connection_id}-{}-{}-{section_name}",
            table_path.schema_name, table_path.table_name
        );
        let is_expanded = connection.expanded_table_sections.contains(&section_path);
        let row = h_flex()
            .w_full()
            .items_center()
            .gap_1()
            .child(
                Disclosure::new(section_toggle_id, is_expanded).on_click(cx.listener({
                    let section_path = section_path.clone();
                    move |this, _, _, cx| {
                        this.toggle_table_section_expanded(connection_id, section_path.clone(), cx);
                    }
                })),
            )
            .child(
                h_flex()
                    .id(section_row_id)
                    .flex_1()
                    .min_w_0()
                    .px_2()
                    .py_1()
                    .gap_2()
                    .rounded_md()
                    .hover(|style| style.bg(cx.theme().colors().element_hover))
                    .cursor_pointer()
                    .on_click(cx.listener({
                        let section_path = section_path.clone();
                        move |this, _, _, cx| {
                            this.toggle_table_section_expanded(
                                connection_id,
                                section_path.clone(),
                                cx,
                            );
                        }
                    }))
                    .child(
                        Icon::new(IconName::Folder)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        Label::new(section_name)
                            .size(LabelSize::Small)
                            .color(Color::Default),
                    )
                    .child(
                        Label::new(item_count.to_string())
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    ),
            );

        v_flex()
            .w_full()
            .gap_1()
            .child(row)
            .when(is_expanded, |this| {
                if items.is_empty() {
                    this.child(
                        v_flex().w_full().pl_8().child(
                            Label::new(empty_message)
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        ),
                    )
                } else {
                    this.child(v_flex().w_full().gap_1().pl_8().children(items))
                }
            })
            .into_any_element()
    }

    fn render_table_browser_item(
        &self,
        icon_name: IconName,
        name: &str,
        detail: &str,
        name_color: Color,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        v_flex()
            .w_full()
            .gap_0p5()
            .px_2()
            .py_1()
            .rounded_md()
            .hover(|style| style.bg(cx.theme().colors().element_hover))
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .gap_2()
                    .child(
                        Icon::new(icon_name)
                            .size(IconSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        Label::new(name.to_string())
                            .size(LabelSize::Small)
                            .color(name_color),
                    ),
            )
            .when(!detail.is_empty(), |this| {
                this.child(
                    Label::new(detail.to_string())
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
            })
            .into_any_element()
    }

    fn build_connection_context_menu(
        &mut self,
        connection_id: Uuid,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<ContextMenu> {
        let Some(connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
        else {
            return self.build_unavailable_context_menu(window, cx);
        };

        let panel = cx.entity();
        let focus_handle = self.focus_handle.clone();
        let connection_name = connection.saved.name.clone();
        let connection_form = connection_form_from_saved_connection(&connection.saved).ok();
        let refresh_label = match &connection.load_state {
            ConnectionLoadState::Idle => "Connect",
            ConnectionLoadState::Failed(_) => "Retry",
            ConnectionLoadState::Loading => "Connecting...",
            ConnectionLoadState::Loaded(_) => "Refresh",
        };
        let is_loading = matches!(connection.load_state, ConnectionLoadState::Loading);
        let host = connection_form.as_ref().map(|form| form.host.clone());
        let user = connection_form.as_ref().map(|form| form.user.clone());
        let database_name = connection_form
            .as_ref()
            .map(|form| form.database_name.clone())
            .filter(|database_name| !database_name.is_empty());
        let default_query_console_schema = match connection.saved.kind {
            DatabaseKind::MySql => database_name.clone(),
            DatabaseKind::Postgres => None,
        };

        ContextMenu::build(window, cx, move |menu, _, _| {
            let menu = menu.context(focus_handle.clone());
            let menu = if is_loading {
                push_disabled_context_menu_entry(menu, refresh_label)
            } else {
                menu.entry(refresh_label, None, {
                    let panel = panel.clone();
                    move |_, cx| {
                        panel.update(cx, |this, cx| {
                            this.refresh_connection(connection_id, cx);
                        });
                    }
                })
            };
            let menu = menu.entry("Edit Connection...", None, {
                let panel = panel.clone();
                move |window, cx| {
                    panel.update(cx, |this, cx| {
                        this.edit_connection(connection_id, window, cx);
                    });
                }
            });
            let menu = menu.submenu("Copy", {
                let panel = panel.clone();
                let connection_name = connection_name.clone();
                let host = host.clone();
                let user = user.clone();
                let database_name = database_name.clone();
                move |menu, _, _| {
                    let mut menu = menu.entry("Name", None, {
                        let panel = panel.clone();
                        let connection_name = connection_name.clone();
                        move |_, cx| {
                            copy_database_panel_value(
                                panel.clone(),
                                "connection name",
                                connection_name.clone(),
                                cx,
                            );
                        }
                    });

                    if let Some(host) = host.clone() {
                        menu = menu.entry("Host", None, {
                            let panel = panel.clone();
                            move |_, cx| {
                                copy_database_panel_value(panel.clone(), "host", host.clone(), cx);
                            }
                        });
                    }

                    if let Some(user) = user.clone() {
                        menu = menu.entry("User", None, {
                            let panel = panel.clone();
                            move |_, cx| {
                                copy_database_panel_value(panel.clone(), "user", user.clone(), cx);
                            }
                        });
                    }

                    if let Some(database_name) = database_name.clone() {
                        menu = menu.entry("Database", None, {
                            let panel = panel.clone();
                            move |_, cx| {
                                copy_database_panel_value(
                                    panel.clone(),
                                    "database name",
                                    database_name.clone(),
                                    cx,
                                );
                            }
                        });
                    }

                    menu
                }
            });
            let new_menu_panel = panel.clone();
            let new_menu_default_query_console_schema = default_query_console_schema.clone();
            let menu = menu.separator().submenu("New", move |menu, _, _| {
                let menu = menu.entry("Query Console", None, {
                    let panel = new_menu_panel.clone();
                    let default_query_console_schema =
                        new_menu_default_query_console_schema.clone();
                    move |window, cx| {
                        panel.update(cx, |this, cx| {
                            this.open_query_console(
                                connection_id,
                                default_query_console_schema.clone(),
                                None,
                                None,
                                window,
                                cx,
                            );
                        });
                    }
                });
                let menu = push_disabled_context_menu_entry(menu, "Schema");
                let menu = push_disabled_context_menu_entry(menu, "Table");
                push_disabled_context_menu_entry(menu, "View")
            });
            menu.separator().entry("Remove Connection...", None, {
                let panel = panel.clone();
                move |window, cx| {
                    panel.update(cx, |this, cx| {
                        this.delete_connection(connection_id, window, cx);
                    });
                }
            })
        })
    }

    fn build_schema_context_menu(
        &mut self,
        connection_id: Uuid,
        schema_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<ContextMenu> {
        let Some(connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
        else {
            return self.build_unavailable_context_menu(window, cx);
        };

        let panel = cx.entity();
        let focus_handle = self.focus_handle.clone();
        let is_expanded = connection.expanded_schemas.contains(&schema_name);
        let toggle_label = if is_expanded { "Collapse" } else { "Expand" };
        let schema_label = format!("Copy {} DDL", connection.saved.kind.schema_label());

        ContextMenu::build(window, cx, move |menu, _, _| {
            let menu = menu.context(focus_handle.clone());
            let menu = menu.entry("Query Console", None, {
                let panel = panel.clone();
                let schema_name = schema_name.clone();
                move |window, cx| {
                    panel.update(cx, |this, cx| {
                        this.open_query_console(
                            connection_id,
                            Some(schema_name.clone()),
                            None,
                            None,
                            window,
                            cx,
                        );
                    });
                }
            });
            let menu = menu.separator().submenu("New", |menu, _, _| {
                let menu = push_disabled_context_menu_entry(menu, "Table");
                push_disabled_context_menu_entry(menu, "View")
            });
            let menu = menu.separator().entry("Refresh", None, {
                let panel = panel.clone();
                move |_, cx| {
                    panel.update(cx, |this, cx| {
                        this.refresh_connection(connection_id, cx);
                    });
                }
            });
            let menu = menu.entry(toggle_label, None, {
                let panel = panel.clone();
                let schema_name = schema_name.clone();
                move |_, cx| {
                    panel.update(cx, |this, cx| {
                        this.toggle_schema_expanded(connection_id, schema_name.clone(), cx);
                    });
                }
            });
            let menu = menu.separator().entry(&schema_label, None, {
                let panel = panel.clone();
                let schema_name = schema_name.clone();
                move |_, cx| {
                    panel.update(cx, |this, cx| {
                        this.copy_schema_ddl(connection_id, schema_name.clone(), cx);
                    });
                }
            });
            let menu = menu.entry("Copy Name", None, {
                let panel = panel.clone();
                let schema_name = schema_name.clone();
                move |_, cx| {
                    copy_database_panel_value(
                        panel.clone(),
                        "schema name",
                        schema_name.clone(),
                        cx,
                    );
                }
            });
            let menu = menu.separator();
            let menu = push_disabled_context_menu_entry(menu, "Modify Schema...");
            push_disabled_context_menu_entry(menu, "Drop Schema...")
        })
    }

    fn build_table_context_menu(
        &mut self,
        connection_id: Uuid,
        schema_name: String,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<ContextMenu> {
        let Some(connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
        else {
            return self.build_unavailable_context_menu(window, cx);
        };

        let panel = cx.entity();
        let focus_handle = self.focus_handle.clone();
        let qualified_table_name = format!("{schema_name}.{table_name}");
        let connection_kind = connection.saved.kind;
        let supports_table_ddl = connection.saved.kind.supports_table_ddl();
        let supports_table_editor = connection.saved.kind.supports_table_editor();
        let is_expanded = connection.expanded_tables.contains(&DatabaseTablePath::new(
            schema_name.clone(),
            table_name.clone(),
        ));
        let toggle_label = if is_expanded { "Collapse" } else { "Expand" };

        ContextMenu::build(window, cx, move |menu, _, _| {
            let menu = menu.context(focus_handle.clone());
            let menu = menu.entry("Query Console", None, {
                let panel = panel.clone();
                let schema_name = schema_name.clone();
                let table_name = table_name.clone();
                move |window, cx| {
                    panel.update(cx, |this, cx| {
                        this.open_query_console(
                            connection_id,
                            Some(schema_name.clone()),
                            Some(DatabaseTablePath::new(
                                schema_name.clone(),
                                table_name.clone(),
                            )),
                            Some(default_query_console_table_text(
                                connection_kind,
                                &schema_name,
                                &table_name,
                            )),
                            window,
                            cx,
                        );
                    });
                }
            });
            let menu = menu.separator().submenu("New", |menu, _, _| {
                let menu = push_disabled_context_menu_entry(menu, "Column");
                let menu = push_disabled_context_menu_entry(menu, "Index");
                let menu = push_disabled_context_menu_entry(menu, "Foreign Key");
                push_disabled_context_menu_entry(menu, "Trigger")
            });
            let menu = menu.separator();
            let menu = push_disabled_context_menu_entry(menu, "Edit Data");
            let menu = menu.entry("Refresh", None, {
                let panel = panel.clone();
                move |_, cx| {
                    panel.update(cx, |this, cx| {
                        this.refresh_connection(connection_id, cx);
                    });
                }
            });
            let menu = menu.entry(toggle_label, None, {
                let panel = panel.clone();
                let schema_name = schema_name.clone();
                let table_name = table_name.clone();
                move |_, cx| {
                    panel.update(cx, |this, cx| {
                        this.toggle_table_expanded(
                            connection_id,
                            DatabaseTablePath::new(schema_name.clone(), table_name.clone()),
                            cx,
                        );
                    });
                }
            });
            let menu = if supports_table_ddl {
                menu.entry("Copy DDL", None, {
                    let panel = panel.clone();
                    let schema_name = schema_name.clone();
                    let table_name = table_name.clone();
                    move |_, cx| {
                        panel.update(cx, |this, cx| {
                            this.copy_table_ddl(
                                connection_id,
                                schema_name.clone(),
                                table_name.clone(),
                                cx,
                            );
                        });
                    }
                })
            } else {
                push_disabled_context_menu_entry(menu, "Copy DDL")
            };
            let menu = menu.entry("Copy Name", None, {
                let panel = panel.clone();
                let table_name = table_name.clone();
                move |_, cx| {
                    copy_database_panel_value(panel.clone(), "table name", table_name.clone(), cx);
                }
            });
            let menu = menu.entry("Copy Qualified Name", None, {
                let panel = panel.clone();
                let qualified_table_name = qualified_table_name.clone();
                move |_, cx| {
                    copy_database_panel_value(
                        panel.clone(),
                        "qualified table name",
                        qualified_table_name.clone(),
                        cx,
                    );
                }
            });
            let menu = menu.separator();
            let menu = if supports_table_editor {
                menu.entry("Modify Table...", None, {
                    let panel = panel.clone();
                    let schema_name = schema_name.clone();
                    let table_name = table_name.clone();
                    move |window, cx| {
                        panel.update(cx, |this, cx| {
                            this.open_table_editor_modal(
                                connection_id,
                                schema_name.clone(),
                                table_name.clone(),
                                window,
                                cx,
                            );
                        });
                    }
                })
            } else {
                push_disabled_context_menu_entry(menu, "Modify Table...")
            };
            push_disabled_context_menu_entry(menu, "Drop Table...")
        })
    }

    fn build_unavailable_context_menu(
        &self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Entity<ContextMenu> {
        let focus_handle = self.focus_handle.clone();
        ContextMenu::build(window, cx, move |menu, _, _| {
            let menu = menu.context(focus_handle.clone());
            push_disabled_context_menu_entry(menu, "No actions available")
        })
    }

    fn copy_schema_ddl(
        &mut self,
        connection_id: Uuid,
        schema_name: String,
        cx: &mut Context<Self>,
    ) {
        let Some(saved_connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
            .map(|connection| connection.saved.clone())
        else {
            return;
        };

        let connection_url = saved_connection.url.clone();
        let connection_kind = saved_connection.kind;
        let schema_label = connection_kind.schema_label().to_string();
        let schema_name_for_task = schema_name.clone();
        self.notice = Some(PanelNotice {
            kind: NoticeKind::Info,
            message: format!("Loading {} DDL for {}…", schema_label, schema_name),
        });
        cx.notify();

        cx.spawn(async move |this, cx| -> Result<()> {
            match Tokio::spawn_result(cx, async move {
                load_schema_ddl(
                    connection_kind,
                    connection_url.clone(),
                    schema_name_for_task,
                )
                .await
            })
            .await
            {
                Ok(ddl) => {
                    this.update(cx, |this, cx| {
                        cx.write_to_clipboard(ClipboardItem::new_string(ddl));
                        this.notice = Some(PanelNotice {
                            kind: NoticeKind::Success,
                            message: format!("Copied {} DDL for {}.", schema_label, schema_name),
                        });
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    let error_message = format_connection_error(&error, &saved_connection.url);
                    this.update(cx, |this, cx| {
                        this.notice = Some(PanelNotice {
                            kind: NoticeKind::Error,
                            message: format!(
                                "Failed to copy {} DDL for {}. {}",
                                schema_label, schema_name, error_message
                            ),
                        });
                        cx.notify();
                    })?;
                }
            }

            Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn copy_table_ddl(
        &mut self,
        connection_id: Uuid,
        schema_name: String,
        table_name: String,
        cx: &mut Context<Self>,
    ) {
        let Some(saved_connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
            .map(|connection| connection.saved.clone())
        else {
            return;
        };

        let connection_url = saved_connection.url.clone();
        let connection_kind = saved_connection.kind;
        let schema_name_for_task = schema_name.clone();
        let table_name_for_task = table_name.clone();
        self.notice = Some(PanelNotice {
            kind: NoticeKind::Info,
            message: format!("Loading DDL for {}.{}…", schema_name, table_name),
        });
        cx.notify();

        cx.spawn(async move |this, cx| -> Result<()> {
            match Tokio::spawn_result(cx, async move {
                load_table_ddl(
                    connection_kind,
                    connection_url.clone(),
                    schema_name_for_task,
                    table_name_for_task,
                )
                .await
            })
            .await
            {
                Ok(ddl) => {
                    this.update(cx, |this, cx| {
                        cx.write_to_clipboard(ClipboardItem::new_string(ddl));
                        this.notice = Some(PanelNotice {
                            kind: NoticeKind::Success,
                            message: format!("Copied DDL for {}.{}.", schema_name, table_name),
                        });
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    let error_message = format_connection_error(&error, &saved_connection.url);
                    this.update(cx, |this, cx| {
                        this.notice = Some(PanelNotice {
                            kind: NoticeKind::Error,
                            message: format!(
                                "Failed to copy DDL for {}.{}. {}",
                                schema_name, table_name, error_message
                            ),
                        });
                        cx.notify();
                    })?;
                }
            }

            Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn open_table_editor_modal(
        &mut self,
        connection_id: Uuid,
        schema_name: String,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(saved_connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
            .map(|connection| connection.saved.clone())
        else {
            return;
        };

        if !saved_connection.kind.supports_table_editor() {
            self.notice = Some(PanelNotice {
                kind: NoticeKind::Error,
                message: format!(
                    "Modify Table is not supported yet for {} connections.",
                    saved_connection.kind.label()
                ),
            });
            cx.notify();
            return;
        }

        let panel = cx.entity().downgrade();
        let workspace = self.workspace.clone();
        let open_result = workspace.update(cx, |workspace, cx| {
            workspace.toggle_modal(window, cx, |window, cx| {
                let modal = DatabaseTableEditorModal::new(
                    panel.clone(),
                    connection_id,
                    saved_connection.clone(),
                    schema_name.clone(),
                    table_name.clone(),
                    window,
                    cx,
                );
                window.focus(&modal.focus_handle(cx), cx);
                modal
            });
        });

        if open_result.is_err() {
            self.notice = Some(PanelNotice {
                kind: NoticeKind::Error,
                message: "Could not open the table editor.".to_string(),
            });
            cx.notify();
        }
    }

    fn open_query_console(
        &mut self,
        connection_id: Uuid,
        schema_name: Option<String>,
        target_table_path: Option<DatabaseTablePath>,
        initial_text: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(saved_connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
            .map(|connection| connection.saved.clone())
        else {
            return;
        };

        let workspace = self.workspace.clone();
        let open_result = workspace.update(cx, |workspace, cx| {
            let query_console = cx.new(|cx| {
                DatabaseQueryConsole::new(
                    saved_connection.clone(),
                    schema_name.clone(),
                    target_table_path.clone(),
                    initial_text.clone(),
                    window,
                    cx,
                )
            });
            workspace.add_item_to_active_pane(
                Box::new(query_console.clone()),
                None,
                true,
                window,
                cx,
            );
            query_console.focus_handle(cx).focus(window, cx);
        });

        if open_result.is_err() {
            self.notice = Some(PanelNotice {
                kind: NoticeKind::Error,
                message: "Could not open the query console.".to_string(),
            });
            cx.notify();
        }
    }

    fn submit_connection_form(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let connection_form = self.read_connection_form(cx);
        let connection_name = self.name_editor.read(cx).text(cx).trim().to_string();
        let kind = connection_form.kind;

        let (normalized_url, parsed_url) = match build_connection_url(&connection_form) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.notice = Some(PanelNotice {
                    kind: NoticeKind::Error,
                    message: error.to_string(),
                });
                cx.notify();
                return;
            }
        };

        if self.connections.iter().any(|connection| {
            connection.saved.url == normalized_url
                && Some(connection.saved.id) != self.editing_connection_id
        }) {
            self.notice = Some(PanelNotice {
                kind: NoticeKind::Error,
                message: "This connection URL is already saved.".to_string(),
            });
            cx.notify();
            return;
        }

        if let Some(connection_id) = self.editing_connection_id {
            let resolved_connection_name = if connection_name.is_empty() {
                default_connection_name(&parsed_url, kind)
            } else {
                connection_name
            };
            let Some((updated_connection_name, should_refresh)) = self
                .connections
                .iter_mut()
                .find(|connection| connection.saved.id == connection_id)
                .map(|connection| {
                    let should_refresh =
                        connection.saved.kind != kind || connection.saved.url != normalized_url;
                    connection.saved.kind = kind;
                    connection.saved.name = resolved_connection_name.clone();
                    connection.saved.url = normalized_url.clone();
                    connection.expanded = true;
                    if should_refresh {
                        connection.expanded_schemas.clear();
                        connection.expanded_tables.clear();
                        connection.expanded_table_sections.clear();
                        connection.table_browser_load_states.clear();
                        connection.load_state = ConnectionLoadState::Idle;
                    }
                    (connection.saved.name.clone(), should_refresh)
                })
            else {
                self.notice = Some(PanelNotice {
                    kind: NoticeKind::Error,
                    message: "Could not find the connection being edited.".to_string(),
                });
                cx.notify();
                return;
            };

            self.close_connection_form(window, cx);
            self.notice = Some(PanelNotice {
                kind: NoticeKind::Info,
                message: if should_refresh {
                    format!("Updated {}. Connecting…", updated_connection_name)
                } else {
                    format!("Updated {}.", updated_connection_name)
                },
            });
            self.persist_connections(cx);
            if should_refresh {
                self.refresh_connection(connection_id, cx);
            } else {
                cx.notify();
            }
            return;
        }

        let saved_connection = SavedDatabaseConnection {
            id: Uuid::new_v4(),
            kind,
            name: if connection_name.is_empty() {
                default_connection_name(&parsed_url, kind)
            } else {
                connection_name
            },
            url: normalized_url,
        };
        let connection_id = saved_connection.id;
        let saved_connection_name = saved_connection.name.clone();

        let mut connection_entry = DatabaseConnectionEntry::new(saved_connection);
        connection_entry.expanded = true;
        self.connections.insert(0, connection_entry);
        self.close_connection_form(window, cx);
        self.notice = Some(PanelNotice {
            kind: NoticeKind::Info,
            message: format!("Saved {}. Connecting…", saved_connection_name),
        });
        self.persist_connections(cx);
        self.refresh_connection(connection_id, cx);
        cx.notify();
    }

    fn toggle_connection_expanded(&mut self, connection_id: Uuid, cx: &mut Context<Self>) {
        let mut should_refresh = false;
        if let Some(connection) = self
            .connections
            .iter_mut()
            .find(|connection| connection.saved.id == connection_id)
        {
            connection.expanded = !connection.expanded;
            should_refresh = connection.expanded
                && !matches!(&connection.load_state, ConnectionLoadState::Loaded(_))
                && !matches!(&connection.load_state, ConnectionLoadState::Loading);
        }

        if should_refresh {
            self.refresh_connection(connection_id, cx);
        } else {
            cx.notify();
        }
    }

    fn toggle_schema_expanded(
        &mut self,
        connection_id: Uuid,
        schema_name: String,
        cx: &mut Context<Self>,
    ) {
        if let Some(connection) = self
            .connections
            .iter_mut()
            .find(|connection| connection.saved.id == connection_id)
        {
            if connection.expanded_schemas.contains(&schema_name) {
                connection.expanded_schemas.remove(&schema_name);
            } else {
                connection.expanded_schemas.insert(schema_name);
            }
            cx.notify();
        }
    }

    fn toggle_table_expanded(
        &mut self,
        connection_id: Uuid,
        table_path: DatabaseTablePath,
        cx: &mut Context<Self>,
    ) {
        let mut should_load = false;
        if let Some(connection) = self
            .connections
            .iter_mut()
            .find(|connection| connection.saved.id == connection_id)
        {
            if connection.expanded_tables.contains(&table_path) {
                connection.expanded_tables.remove(&table_path);
            } else {
                connection.expanded_tables.insert(table_path.clone());
                should_load = !matches!(
                    connection.table_browser_load_states.get(&table_path),
                    Some(TableBrowserLoadState::Loading) | Some(TableBrowserLoadState::Loaded(_))
                );
            }
        }

        if should_load {
            self.load_table_browser_details_for_table(connection_id, table_path, cx);
        } else {
            cx.notify();
        }
    }

    fn toggle_table_section_expanded(
        &mut self,
        connection_id: Uuid,
        section_path: DatabaseTableSectionPath,
        cx: &mut Context<Self>,
    ) {
        if let Some(connection) = self
            .connections
            .iter_mut()
            .find(|connection| connection.saved.id == connection_id)
        {
            if connection.expanded_table_sections.contains(&section_path) {
                connection.expanded_table_sections.remove(&section_path);
            } else {
                connection.expanded_table_sections.insert(section_path);
            }
            cx.notify();
        }
    }

    fn load_table_browser_details_for_table(
        &mut self,
        connection_id: Uuid,
        table_path: DatabaseTablePath,
        cx: &mut Context<Self>,
    ) {
        let Some(saved_connection) = self
            .connections
            .iter()
            .find(|connection| connection.saved.id == connection_id)
            .map(|connection| connection.saved.clone())
        else {
            return;
        };

        if let Some(connection) = self
            .connections
            .iter_mut()
            .find(|connection| connection.saved.id == connection_id)
        {
            connection
                .table_browser_load_states
                .insert(table_path.clone(), TableBrowserLoadState::Loading);
        }
        cx.notify();

        let connection_url = saved_connection.url.clone();
        let schema_name_for_task = table_path.schema_name.clone();
        let table_name_for_task = table_path.table_name.clone();
        let connection_name = saved_connection.name.clone();
        let table_path_for_task = table_path.clone();
        cx.spawn(async move |this, cx| -> Result<()> {
            match Tokio::spawn_result(cx, async move {
                load_table_browser_details(
                    saved_connection.kind,
                    connection_url.clone(),
                    schema_name_for_task,
                    table_name_for_task,
                )
                .await
            })
            .await
            {
                Ok(details) => {
                    this.update(cx, |this, cx| {
                        if let Some(connection) = this
                            .connections
                            .iter_mut()
                            .find(|connection| connection.saved.id == connection_id)
                        {
                            if matches!(
                                connection
                                    .table_browser_load_states
                                    .get(&table_path_for_task),
                                Some(TableBrowserLoadState::Loading)
                            ) {
                                connection.table_browser_load_states.insert(
                                    table_path_for_task.clone(),
                                    TableBrowserLoadState::Loaded(details),
                                );
                                cx.notify();
                            }
                        }
                    })?;
                }
                Err(error) => {
                    let error_message = format_connection_error(&error, &saved_connection.url);
                    log::error!(
                        "Failed to load table structure for {}.{} on {}: {}",
                        table_path.schema_name,
                        table_path.table_name,
                        connection_name,
                        error_message
                    );
                    this.update(cx, |this, cx| {
                        if let Some(connection) = this
                            .connections
                            .iter_mut()
                            .find(|connection| connection.saved.id == connection_id)
                        {
                            if matches!(
                                connection.table_browser_load_states.get(&table_path),
                                Some(TableBrowserLoadState::Loading)
                            ) {
                                connection.table_browser_load_states.insert(
                                    table_path.clone(),
                                    TableBrowserLoadState::Failed(error_message.clone()),
                                );
                                this.notice = Some(PanelNotice {
                                    kind: NoticeKind::Error,
                                    message: format!(
                                        "Failed to load structure for {}.{}. {}",
                                        table_path.schema_name,
                                        table_path.table_name,
                                        error_message
                                    ),
                                });
                                cx.notify();
                            }
                        }
                    })?;
                }
            }

            Ok(())
        })
        .detach_and_log_err(cx);
    }

    pub fn load(
        workspace: WeakEntity<Workspace>,
        cx: AsyncWindowContext,
    ) -> Task<Result<Entity<Self>>> {
        cx.spawn(async move |cx| {
            let saved_connections = cx.update(|_, cx| Self::read_saved_connections(cx))?;
            let workspace_for_panel = workspace.clone();
            workspace.update_in(cx, |_workspace, window, cx| {
                cx.new(|cx| Self::new(workspace_for_panel, saved_connections, window, cx))
            })
        })
    }
}

impl EventEmitter<PanelEvent> for DatabasePanel {}

impl Focusable for DatabasePanel {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus_handle.clone()
    }
}

impl Panel for DatabasePanel {
    fn persistent_name() -> &'static str {
        "DatabasePanel"
    }

    fn panel_key() -> &'static str {
        DATABASE_PANEL_KEY
    }

    fn position(&self, _window: &Window, _cx: &App) -> DockPosition {
        DockPosition::Right
    }

    fn position_is_valid(&self, position: DockPosition) -> bool {
        position == DockPosition::Right
    }

    fn set_position(
        &mut self,
        _position: DockPosition,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) {
    }

    fn default_size(&self, _window: &Window, cx: &App) -> Pixels {
        AgentSettings::get_global(cx).default_width
    }

    fn size_persistence_mode(&self, _window: &Window, _cx: &App) -> PanelSizePersistence {
        PanelSizePersistence::Global
    }

    fn icon(&self, _window: &Window, _cx: &App) -> Option<IconName> {
        Some(IconName::DatabaseZap)
    }

    fn icon_tooltip(&self, _window: &Window, _cx: &App) -> Option<&'static str> {
        Some("Database Panel")
    }

    fn toggle_action(&self) -> Box<dyn gpui::Action> {
        Box::new(ToggleFocus)
    }

    fn is_zoomed(&self, _window: &Window, _cx: &App) -> bool {
        self.zoomed
    }

    fn set_zoomed(&mut self, zoomed: bool, _window: &mut Window, cx: &mut Context<Self>) {
        self.zoomed = zoomed;
        cx.notify();
    }

    fn activation_priority(&self) -> u32 {
        8
    }
}

impl Render for DatabasePanel {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let content = if self.connections.is_empty() {
            self.render_empty_state(cx).into_any_element()
        } else {
            v_flex()
                .w_full()
                .gap_2()
                .children(
                    self.connections
                        .iter()
                        .map(|connection| self.render_connection(connection, cx)),
                )
                .into_any_element()
        };

        v_flex()
            .size_full()
            .track_focus(&self.focus_handle(cx))
            .on_action(cx.listener(|this, _: &menu::Cancel, window, cx| {
                if this.show_add_connection_form {
                    this.close_connection_form(window, cx);
                    cx.notify();
                }
            }))
            .on_action(cx.listener(|this, _: &menu::Confirm, window, cx| {
                if this.show_add_connection_form {
                    this.submit_connection_form(window, cx);
                }
            }))
            .child(
                h_flex()
                    .h(px(40.))
                    .w_full()
                    .px_3()
                    .justify_between()
                    .items_center()
                    .border_b_1()
                    .border_color(cx.theme().colors().border)
                    .child(
                        h_flex()
                            .gap_2()
                            .items_center()
                            .child(
                                Icon::new(IconName::DatabaseZap)
                                    .size(IconSize::Medium)
                                    .color(Color::Muted),
                            )
                            .child(Label::new("Database")),
                    )
                    .child(
                        h_flex()
                            .gap_1()
                            .child(
                                IconButton::new("refresh-database-connections", IconName::RotateCw)
                                    .shape(IconButtonShape::Square)
                                    .icon_size(IconSize::Small)
                                    .disabled(self.connections.is_empty())
                                    .tooltip(Tooltip::text("Refresh saved connections"))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.refresh_all_connections(cx);
                                    })),
                            )
                            .child(
                                IconButton::new("add-database-connection", IconName::Plus)
                                    .shape(IconButtonShape::Square)
                                    .icon_size(IconSize::Small)
                                    .toggle_state(self.show_add_connection_form)
                                    .tooltip(Tooltip::text("Add database connection"))
                                    .on_click(cx.listener(|this, _, window, cx| {
                                        if this.show_add_connection_form {
                                            this.close_connection_form(window, cx);
                                        } else {
                                            this.open_new_connection_form(window, cx);
                                        }
                                    })),
                            )
                            .child(
                                IconButton::new("close-database-panel", IconName::Close)
                                    .shape(IconButtonShape::Square)
                                    .icon_size(IconSize::Small)
                                    .tooltip(Tooltip::text("Close database panel"))
                                    .on_click(|_, window, cx| {
                                        window.dispatch_action(
                                            Box::new(workspace::ToggleRightDock),
                                            cx,
                                        );
                                    }),
                            ),
                    ),
            )
            .child(
                v_flex()
                    .id("database-panel-content")
                    .flex_1()
                    .overflow_y_scroll()
                    .gap_3()
                    .p_3()
                    .children(self.render_notice(cx))
                    .when(self.show_add_connection_form, |this| {
                        this.child(self.render_add_connection_form(window, cx))
                    })
                    .child(content),
            )
    }
}

fn connection_subtitle(connection: &SavedDatabaseConnection) -> String {
    let Ok(parsed_url) = Url::parse(&connection.url) else {
        return connection.kind.label().to_string();
    };
    let host = parsed_url.host_str().unwrap_or("unknown-host");
    let port = parsed_url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    let database_name = decode_connection_component(parsed_url.path().trim_start_matches('/'));
    if database_name.is_empty() {
        format!("{} · {host}{port}", connection.kind.label())
    } else {
        format!(
            "{} · {host}{port} / {database_name}",
            connection.kind.label()
        )
    }
}

fn connection_load_state_status(load_state: &ConnectionLoadState) -> Option<(&'static str, Color)> {
    match load_state {
        ConnectionLoadState::Idle => None,
        ConnectionLoadState::Loading => Some(("Connecting...", Color::Muted)),
        ConnectionLoadState::Loaded(_) => Some(("Connected", Color::Success)),
        ConnectionLoadState::Failed(_) => Some(("Connection failed", Color::Error)),
    }
}

fn default_connection_name(parsed_url: &Url, kind: DatabaseKind) -> String {
    let host = parsed_url.host_str().unwrap_or(kind.label());
    let database_name = decode_connection_component(parsed_url.path().trim_start_matches('/'));
    if database_name.is_empty() {
        host.to_string()
    } else {
        format!("{host} / {database_name}")
    }
}

fn default_expanded_schemas(kind: DatabaseKind, metadata: &DatabaseMetadata) -> HashSet<String> {
    let mut expanded_schemas = HashSet::default();
    let default_schema = match kind {
        DatabaseKind::Postgres => metadata
            .schemas
            .iter()
            .find(|schema| schema.name == "public")
            .or_else(|| metadata.schemas.first()),
        DatabaseKind::MySql => metadata
            .schemas
            .iter()
            .find(|schema| schema.name != "information_schema")
            .or_else(|| metadata.schemas.first()),
    };

    if let Some(schema) = default_schema {
        expanded_schemas.insert(schema.name.clone());
    }

    expanded_schemas
}

async fn load_connection_metadata(
    kind: DatabaseKind,
    connection_url: String,
) -> Result<DatabaseMetadata> {
    match kind {
        DatabaseKind::Postgres => load_postgres_metadata(&connection_url).await,
        DatabaseKind::MySql => load_mysql_metadata(&connection_url).await,
    }
}

async fn load_mysql_metadata(connection_url: &str) -> Result<DatabaseMetadata> {
    let mut connection = MySqlConnection::connect(connection_url)
        .await
        .context("Could not connect to MySQL")?;
    let schema_names = match selected_mysql_schema_name(connection_url)? {
        Some(schema_name) => vec![schema_name],
        None => load_mysql_schema_names(&mut connection).await?,
    };

    let mut schemas = Vec::with_capacity(schema_names.len());
    for schema_name in schema_names {
        let tables = load_mysql_tables(&mut connection, &schema_name).await?;
        schemas.push(DatabaseSchema {
            name: schema_name,
            tables,
        });
    }

    Ok(DatabaseMetadata { schemas })
}

async fn load_mysql_schema_names(connection: &mut MySqlConnection) -> Result<Vec<String>> {
    match query(
        r#"
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('mysql', 'performance_schema', 'sys')
        ORDER BY schema_name
        "#,
    )
    .fetch_all(&mut *connection)
    .await
    {
        Ok(rows) => {
            let mut schema_names = Vec::with_capacity(rows.len());
            for row in rows {
                let schema_name = decode_mysql_text_column(&row, "schema name")?;
                schema_names.push(schema_name);
            }
            Ok(schema_names)
        }
        Err(information_schema_error) => {
            let rows = query("SHOW DATABASES")
                .fetch_all(&mut *connection)
                .await
                .with_context(|| {
                    format!(
                        "Could not load schemas with SHOW DATABASES after information_schema.schemata failed: {information_schema_error}"
                    )
                })?;

            let mut schema_names = Vec::with_capacity(rows.len());
            for row in rows {
                let schema_name =
                    decode_mysql_text_column(&row, "schema name from SHOW DATABASES")?;
                if !matches!(schema_name.as_str(), "mysql" | "performance_schema" | "sys") {
                    schema_names.push(schema_name);
                }
            }
            schema_names.sort();
            Ok(schema_names)
        }
    }
}

async fn load_mysql_tables(
    connection: &mut MySqlConnection,
    schema_name: &str,
) -> Result<Vec<String>> {
    match query(
        r#"
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        "#,
    )
    .bind(schema_name)
    .fetch_all(&mut *connection)
    .await
    {
        Ok(rows) => {
            let mut tables = Vec::with_capacity(rows.len());
            for row in rows {
                let table_name = decode_mysql_text_column(
                    &row,
                    &format!("table name for schema {schema_name}"),
                )?;
                tables.push(table_name);
            }
            Ok(tables)
        }
        Err(information_schema_error) => {
            let statement = format!(
                "SHOW FULL TABLES FROM {} WHERE Table_type = 'BASE TABLE'",
                quote_mysql_identifier(schema_name)
            );
            let rows = query(&statement)
                .fetch_all(&mut *connection)
                .await
                .with_context(|| {
                    format!(
                        "Could not load tables for schema {schema_name} with SHOW FULL TABLES after information_schema.tables failed: {information_schema_error}"
                    )
                })?;

            let mut tables = Vec::with_capacity(rows.len());
            for row in rows {
                let table_name = decode_mysql_text_column(
                    &row,
                    &format!("table name for schema {schema_name}"),
                )?;
                tables.push(table_name);
            }
            tables.sort();
            Ok(tables)
        }
    }
}

async fn load_postgres_metadata(connection_url: &str) -> Result<DatabaseMetadata> {
    let mut connection = PgConnection::connect(connection_url)
        .await
        .context("Could not connect to Postgres")?;
    let schema_names = query_scalar::<_, String>(
        r#"
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog')
          AND schema_name NOT LIKE 'pg_toast%'
        ORDER BY schema_name
        "#,
    )
    .fetch_all(&mut connection)
    .await
    .context("Could not load schemas")?;

    let mut schemas = Vec::with_capacity(schema_names.len());
    for schema_name in schema_names {
        let tables = query_scalar::<_, String>(
            r#"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            "#,
        )
        .bind(&schema_name)
        .fetch_all(&mut connection)
        .await
        .with_context(|| format!("Could not load tables for schema {}", schema_name))?;
        schemas.push(DatabaseSchema {
            name: schema_name,
            tables,
        });
    }

    Ok(DatabaseMetadata { schemas })
}

async fn load_schema_ddl(
    kind: DatabaseKind,
    connection_url: String,
    schema_name: String,
) -> Result<String> {
    match kind {
        DatabaseKind::Postgres => Ok(format!(
            "CREATE SCHEMA IF NOT EXISTS {};",
            quote_postgres_identifier(&schema_name)
        )),
        DatabaseKind::MySql => load_mysql_schema_ddl(&connection_url, &schema_name).await,
    }
}

async fn load_table_ddl(
    kind: DatabaseKind,
    connection_url: String,
    schema_name: String,
    table_name: String,
) -> Result<String> {
    match kind {
        DatabaseKind::Postgres => Err(anyhow!(
            "Copy DDL for tables is not supported yet for Postgres."
        )),
        DatabaseKind::MySql => {
            load_mysql_table_ddl(&connection_url, &schema_name, &table_name).await
        }
    }
}

async fn load_table_browser_details(
    kind: DatabaseKind,
    connection_url: String,
    schema_name: String,
    table_name: String,
) -> Result<DatabaseTableBrowserDetails> {
    match kind {
        DatabaseKind::Postgres => {
            load_postgres_table_browser_details(&connection_url, &schema_name, &table_name).await
        }
        DatabaseKind::MySql => {
            load_mysql_table_browser_details(&connection_url, &schema_name, &table_name).await
        }
    }
}

async fn load_mysql_table_browser_details(
    connection_url: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<DatabaseTableBrowserDetails> {
    let mut connection = MySqlConnection::connect(connection_url)
        .await
        .context("Could not connect to MySQL")?;

    let column_rows = query(
        r#"
        SELECT
            column_name,
            column_type,
            is_nullable,
            column_key,
            extra,
            column_default,
            column_comment
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load columns for table {schema_name}.{table_name}"))?;

    let mut columns = Vec::with_capacity(column_rows.len());
    for row in column_rows {
        columns.push(describe_mysql_column(&row)?);
    }

    let key_rows = query(
        r#"
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            GROUP_CONCAT(kcu.column_name ORDER BY kcu.ordinal_position SEPARATOR ', ') AS column_names,
            MAX(kcu.referenced_table_schema) AS referenced_table_schema,
            MAX(kcu.referenced_table_name) AS referenced_table_name,
            GROUP_CONCAT(kcu.referenced_column_name ORDER BY kcu.ordinal_position SEPARATOR ', ') AS referenced_column_names
        FROM information_schema.table_constraints AS tc
        LEFT JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_schema = kcu.constraint_schema
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
         AND tc.constraint_name = kcu.constraint_name
        WHERE tc.table_schema = ?
          AND tc.table_name = ?
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
        GROUP BY tc.constraint_name, tc.constraint_type
        ORDER BY
            CASE tc.constraint_type
                WHEN 'PRIMARY KEY' THEN 0
                WHEN 'UNIQUE' THEN 1
                ELSE 2
            END,
            tc.constraint_name
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load keys for table {schema_name}.{table_name}"))?;

    let mut keys = Vec::with_capacity(key_rows.len());
    for row in key_rows {
        keys.push(describe_mysql_key(&row)?);
    }

    let index_rows = query(
        r#"
        SELECT
            index_name,
            non_unique,
            index_type,
            GROUP_CONCAT(column_name ORDER BY seq_in_index SEPARATOR ', ') AS column_names
        FROM information_schema.statistics
        WHERE table_schema = ?
          AND table_name = ?
        GROUP BY index_name, non_unique, index_type
        ORDER BY (index_name = 'PRIMARY') DESC, index_name
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load indexes for table {schema_name}.{table_name}"))?;

    let mut indexes = Vec::with_capacity(index_rows.len());
    for row in index_rows {
        indexes.push(describe_mysql_index(&row)?);
    }

    Ok(DatabaseTableBrowserDetails {
        columns,
        keys,
        indexes,
    })
}

async fn load_postgres_table_browser_details(
    connection_url: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<DatabaseTableBrowserDetails> {
    let mut connection = PgConnection::connect(connection_url)
        .await
        .context("Could not connect to Postgres")?;

    let column_rows = query(
        r#"
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
            a.attnotnull AS is_not_null,
            pg_get_expr(def.adbin, def.adrelid) AS column_default
        FROM pg_attribute AS a
        INNER JOIN pg_class AS rel
            ON rel.oid = a.attrelid
        INNER JOIN pg_namespace AS nsp
            ON nsp.oid = rel.relnamespace
        LEFT JOIN pg_attrdef AS def
            ON def.adrelid = a.attrelid
           AND def.adnum = a.attnum
        WHERE nsp.nspname = $1
          AND rel.relname = $2
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load columns for table {schema_name}.{table_name}"))?;

    let mut columns = Vec::with_capacity(column_rows.len());
    for row in column_rows {
        columns.push(describe_postgres_column(&row)?);
    }

    let key_rows = query(
        r#"
        SELECT
            con.conname AS constraint_name,
            CASE con.contype
                WHEN 'p' THEN 'PRIMARY KEY'
                WHEN 'u' THEN 'UNIQUE'
                WHEN 'f' THEN 'FOREIGN KEY'
                ELSE con.contype::text
            END AS constraint_type,
            pg_get_constraintdef(con.oid, true) AS constraint_definition,
            COALESCE(
                string_agg(attribute.attname, ', ' ORDER BY key_columns.ordinality),
                ''
            ) AS column_names
        FROM pg_constraint AS con
        INNER JOIN pg_class AS rel
            ON rel.oid = con.conrelid
        INNER JOIN pg_namespace AS nsp
            ON nsp.oid = rel.relnamespace
        LEFT JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS key_columns(attnum, ordinality)
            ON TRUE
        LEFT JOIN pg_attribute AS attribute
            ON attribute.attrelid = con.conrelid
           AND attribute.attnum = key_columns.attnum
        WHERE nsp.nspname = $1
          AND rel.relname = $2
          AND con.contype IN ('p', 'u', 'f')
        GROUP BY con.oid, con.conname, con.contype
        ORDER BY
            CASE con.contype
                WHEN 'p' THEN 0
                WHEN 'u' THEN 1
                ELSE 2
            END,
            con.conname
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load keys for table {schema_name}.{table_name}"))?;

    let mut keys = Vec::with_capacity(key_rows.len());
    for row in key_rows {
        keys.push(describe_postgres_key(&row)?);
    }

    let index_rows = query(
        r#"
        SELECT
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname = $1
          AND tablename = $2
        ORDER BY indexname
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load indexes for table {schema_name}.{table_name}"))?;

    let mut indexes = Vec::with_capacity(index_rows.len());
    for row in index_rows {
        indexes.push(describe_postgres_index(&row)?);
    }

    Ok(DatabaseTableBrowserDetails {
        columns,
        keys,
        indexes,
    })
}

async fn load_table_editor_details(
    kind: DatabaseKind,
    connection_url: String,
    schema_name: String,
    table_name: String,
) -> Result<DatabaseTableDetails> {
    match kind {
        DatabaseKind::Postgres => Err(anyhow!("Table editing is not supported yet for Postgres.")),
        DatabaseKind::MySql => {
            load_mysql_table_editor_details(&connection_url, &schema_name, &table_name).await
        }
    }
}

async fn apply_table_edit(
    kind: DatabaseKind,
    connection_url: String,
    plan: MySqlTableEditPlan,
) -> Result<()> {
    match kind {
        DatabaseKind::Postgres => Err(anyhow!("Table editing is not supported yet for Postgres.")),
        DatabaseKind::MySql => apply_mysql_table_edit(&connection_url, &plan).await,
    }
}

async fn load_mysql_schema_ddl(connection_url: &str, schema_name: &str) -> Result<String> {
    let mut connection = MySqlConnection::connect(connection_url)
        .await
        .context("Could not connect to MySQL")?;
    load_mysql_schema_ddl_from_connection(&mut connection, schema_name).await
}

async fn load_mysql_schema_ddl_from_connection(
    connection: &mut MySqlConnection,
    schema_name: &str,
) -> Result<String> {
    let statement = format!(
        "SHOW CREATE DATABASE {}",
        quote_mysql_identifier(schema_name)
    );
    let row = query(&statement)
        .fetch_one(&mut *connection)
        .await
        .with_context(|| format!("Could not load DDL for database {schema_name}"))?;

    decode_mysql_text_column_at(&row, 1, &format!("database DDL for {schema_name}"))
}

async fn load_mysql_table_ddl(
    connection_url: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<String> {
    let mut connection = MySqlConnection::connect(connection_url)
        .await
        .context("Could not connect to MySQL")?;
    load_mysql_table_ddl_from_connection(&mut connection, schema_name, table_name).await
}

async fn load_mysql_table_ddl_from_connection(
    connection: &mut MySqlConnection,
    schema_name: &str,
    table_name: &str,
) -> Result<String> {
    let qualified_table_name = format_mysql_qualified_name(schema_name, table_name);
    let statement = format!("SHOW CREATE TABLE {qualified_table_name}");
    let row = query(&statement)
        .fetch_one(&mut *connection)
        .await
        .with_context(|| format!("Could not load DDL for table {schema_name}.{table_name}"))?;

    decode_mysql_text_column_at(
        &row,
        1,
        &format!("table DDL for {schema_name}.{table_name}"),
    )
}

async fn load_mysql_table_editor_details(
    connection_url: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<DatabaseTableDetails> {
    let mut connection = MySqlConnection::connect(connection_url)
        .await
        .context("Could not connect to MySQL")?;
    let table_row = query(
        r#"
        SELECT engine, table_collation, table_comment, create_options
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
          AND table_type = 'BASE TABLE'
        LIMIT 1
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_optional(&mut connection)
    .await
    .with_context(|| format!("Could not load table details for {schema_name}.{table_name}"))?
    .with_context(|| format!("Could not find table {schema_name}.{table_name}"))?;

    let engine = decode_mysql_optional_text_column_at(
        &table_row,
        0,
        &format!("engine for table {schema_name}.{table_name}"),
    )?
    .unwrap_or_default();
    let collation = decode_mysql_optional_text_column_at(
        &table_row,
        1,
        &format!("collation for table {schema_name}.{table_name}"),
    )?
    .unwrap_or_default();
    let comment = decode_mysql_optional_text_column_at(
        &table_row,
        2,
        &format!("comment for table {schema_name}.{table_name}"),
    )?
    .unwrap_or_default();
    let create_options = decode_mysql_optional_text_column_at(
        &table_row,
        3,
        &format!("create options for table {schema_name}.{table_name}"),
    )?
    .unwrap_or_default();

    let column_rows = query(
        r#"
        SELECT
            column_name,
            column_type,
            is_nullable,
            column_key,
            extra,
            column_default,
            column_comment
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
        ORDER BY ordinal_position
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_all(&mut connection)
    .await
    .with_context(|| format!("Could not load columns for table {schema_name}.{table_name}"))?;

    let mut columns = Vec::with_capacity(column_rows.len());
    for row in column_rows {
        columns.push(describe_mysql_column(&row)?);
    }

    let create_table_ddl =
        load_mysql_table_ddl_from_connection(&mut connection, schema_name, table_name).await?;

    Ok(DatabaseTableDetails {
        kind: DatabaseKind::MySql,
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        engine,
        collation,
        comment,
        create_options,
        create_table_ddl,
        columns,
    })
}

async fn apply_mysql_table_edit(connection_url: &str, plan: &MySqlTableEditPlan) -> Result<()> {
    let mut connection = MySqlConnection::connect(connection_url)
        .await
        .context("Could not connect to MySQL")?;

    if let Some(rename_statement) = &plan.rename_statement {
        query(rename_statement)
            .execute(&mut connection)
            .await
            .with_context(|| {
                format!(
                    "Could not rename table to {}.{}",
                    plan.target_schema_name, plan.target_table_name
                )
            })?;
    }

    if let Some(alter_statement) = &plan.alter_statement {
        query(alter_statement)
            .execute(&mut connection)
            .await
            .with_context(|| {
                format!(
                    "Could not update table {}.{}",
                    plan.target_schema_name, plan.target_table_name
                )
            })?;
    }

    Ok(())
}

fn preview_table_edit(
    details: &DatabaseTableDetails,
    values: &TableEditorValues,
) -> Result<String> {
    match details.kind {
        DatabaseKind::Postgres => Ok(details.create_table_ddl.clone()),
        DatabaseKind::MySql => preview_mysql_table_edit(details, values),
    }
}

fn preview_mysql_table_edit(
    details: &DatabaseTableDetails,
    values: &TableEditorValues,
) -> Result<String> {
    let plan = build_mysql_table_edit_plan(details, values)?;
    if plan.is_empty() {
        Ok(details.create_table_ddl.clone())
    } else {
        Ok(plan.preview())
    }
}

fn build_mysql_table_edit_plan(
    details: &DatabaseTableDetails,
    values: &TableEditorValues,
) -> Result<MySqlTableEditPlan> {
    let table_name = values.table_name.trim().to_string();
    if table_name.is_empty() {
        return Err(anyhow!("Table name is required."));
    }

    let engine = values.engine.trim().to_string();
    let collation = values.collation.trim().to_string();
    let comment = values.comment.clone();

    let table_name_changed = table_name != details.table_name;
    let engine_changed = match engine.is_empty() {
        true if details.engine.trim().is_empty() => false,
        true => return Err(anyhow!("Engine is required.")),
        false => !details.engine.eq_ignore_ascii_case(&engine),
    };
    let collation_changed = match collation.is_empty() {
        true if details.collation.trim().is_empty() => false,
        true => return Err(anyhow!("Collation is required.")),
        false => !details.collation.eq_ignore_ascii_case(&collation),
    };
    let comment_changed = comment != details.comment;

    if engine_changed {
        validate_mysql_token("Engine", &engine)?;
    }
    if collation_changed {
        validate_mysql_token("Collation", &collation)?;
    }

    let rename_statement = table_name_changed.then(|| {
        format!(
            "RENAME TABLE {} TO {}",
            format_mysql_qualified_name(&details.schema_name, &details.table_name),
            format_mysql_qualified_name(&details.schema_name, &table_name)
        )
    });

    let mut alter_clauses = Vec::new();
    if engine_changed {
        alter_clauses.push(format!("ENGINE = {engine}"));
    }
    if collation_changed {
        alter_clauses.push(format!("COLLATE = {collation}"));
    }
    if comment_changed {
        alter_clauses.push(format!(
            "COMMENT = {}",
            quote_mysql_string_literal(&comment)
        ));
    }

    let alter_statement = if alter_clauses.is_empty() {
        None
    } else {
        Some(format!(
            "ALTER TABLE {} {}",
            format_mysql_qualified_name(&details.schema_name, &table_name),
            alter_clauses.join(", ")
        ))
    };

    Ok(MySqlTableEditPlan {
        rename_statement,
        alter_statement,
        target_schema_name: details.schema_name.clone(),
        target_table_name: table_name,
    })
}

fn describe_mysql_column(row: &MySqlRow) -> Result<DatabaseTableColumn> {
    let column_name = decode_mysql_text_column_at(row, 0, "column name")?;
    let column_type =
        decode_mysql_text_column_at(row, 1, &format!("column type for {column_name}"))?;
    let is_nullable =
        decode_mysql_text_column_at(row, 2, &format!("nullability for {column_name}"))?;
    let column_key =
        decode_mysql_optional_text_column_at(row, 3, &format!("key type for {column_name}"))?
            .unwrap_or_default();
    let extra =
        decode_mysql_optional_text_column_at(row, 4, &format!("extra data for {column_name}"))?
            .unwrap_or_default();
    let default_value =
        decode_mysql_optional_text_column_at(row, 5, &format!("default value for {column_name}"))?;
    let comment =
        decode_mysql_optional_text_column_at(row, 6, &format!("comment for {column_name}"))?
            .unwrap_or_default();

    let mut parts = Vec::new();
    parts.push(column_type);
    parts.push(if is_nullable.eq_ignore_ascii_case("NO") {
        "NOT NULL".to_string()
    } else {
        "NULL".to_string()
    });
    if let Some(key_label) = format_mysql_column_key(&column_key) {
        parts.push(key_label.to_string());
    }
    if !extra.is_empty() {
        parts.push(extra.replace('_', " ").to_uppercase());
    }
    if let Some(default_value) = default_value {
        if default_value.is_empty() {
            parts.push("DEFAULT ''".to_string());
        } else {
            parts.push(format!("DEFAULT {default_value}"));
        }
    }
    if !comment.is_empty() {
        parts.push(format!("Comment: {comment}"));
    }

    Ok(DatabaseTableColumn {
        name: column_name,
        detail: parts.join(" • "),
    })
}

fn describe_mysql_key(row: &MySqlRow) -> Result<DatabaseTableKey> {
    let constraint_name = decode_mysql_text_column_at(row, 0, "constraint name")?;
    let constraint_type =
        decode_mysql_text_column_at(row, 1, &format!("constraint type for {constraint_name}"))?;
    let column_names =
        decode_mysql_optional_text_column_at(row, 2, &format!("columns for {constraint_name}"))?
            .unwrap_or_default();
    let referenced_schema_name = decode_mysql_optional_text_column_at(
        row,
        3,
        &format!("referenced schema for {constraint_name}"),
    )?;
    let referenced_table_name = decode_mysql_optional_text_column_at(
        row,
        4,
        &format!("referenced table for {constraint_name}"),
    )?;
    let referenced_column_names = decode_mysql_optional_text_column_at(
        row,
        5,
        &format!("referenced columns for {constraint_name}"),
    )?;

    let referenced_table_display = referenced_table_name.map(|referenced_table_name| {
        if let Some(referenced_schema_name) = referenced_schema_name
            .filter(|referenced_schema_name| !referenced_schema_name.is_empty())
        {
            format!("{referenced_schema_name}.{referenced_table_name}")
        } else {
            referenced_table_name
        }
    });

    Ok(DatabaseTableKey {
        name: constraint_name,
        constraint_type: constraint_type.clone(),
        column_names: split_database_object_names(&column_names),
        detail: format_database_table_key_detail(
            &constraint_type,
            &column_names,
            referenced_table_display.as_deref(),
            referenced_column_names.as_deref(),
        ),
    })
}

fn describe_mysql_index(row: &MySqlRow) -> Result<DatabaseTableIndex> {
    let index_name = decode_mysql_text_column_at(row, 0, "index name")?;
    let non_unique = row
        .try_get::<i64, _>(1)
        .with_context(|| format!("Could not decode uniqueness for index {index_name}"))?;
    let index_type =
        decode_mysql_optional_text_column_at(row, 2, &format!("index type for {index_name}"))?
            .unwrap_or_else(|| "INDEX".to_string());
    let column_names =
        decode_mysql_optional_text_column_at(row, 3, &format!("columns for index {index_name}"))?
            .unwrap_or_default();

    let prefix = if index_name.eq_ignore_ascii_case("PRIMARY") {
        "PRIMARY"
    } else if non_unique == 0 {
        "UNIQUE"
    } else {
        "INDEX"
    };
    let detail = if column_names.is_empty() {
        format!("{prefix} {index_type}")
    } else {
        format!("{prefix} {index_type} ({column_names})")
    };

    Ok(DatabaseTableIndex {
        name: index_name,
        detail,
    })
}

fn describe_postgres_column(row: &PgRow) -> Result<DatabaseTableColumn> {
    let column_name = decode_postgres_text_column_at(row, 0, "column name")?;
    let column_type =
        decode_postgres_text_column_at(row, 1, &format!("column type for {column_name}"))?;
    let is_not_null = row
        .try_get::<bool, _>(2)
        .with_context(|| format!("Could not decode nullability for {column_name}"))?;
    let default_value = decode_postgres_optional_text_column_at(
        row,
        3,
        &format!("default value for {column_name}"),
    )?;

    let mut parts = Vec::new();
    parts.push(column_type);
    parts.push(if is_not_null {
        "NOT NULL".to_string()
    } else {
        "NULL".to_string()
    });
    if let Some(default_value) = default_value {
        parts.push(format!("DEFAULT {default_value}"));
    }

    Ok(DatabaseTableColumn {
        name: column_name,
        detail: parts.join(" • "),
    })
}

fn describe_postgres_key(row: &PgRow) -> Result<DatabaseTableKey> {
    let constraint_name = decode_postgres_text_column_at(row, 0, "constraint name")?;
    let constraint_type =
        decode_postgres_text_column_at(row, 1, &format!("constraint type for {constraint_name}"))?;
    let constraint_definition = decode_postgres_text_column_at(
        row,
        2,
        &format!("constraint definition for {constraint_name}"),
    )?;
    let column_names =
        decode_postgres_optional_text_column_at(row, 3, &format!("columns for {constraint_name}"))?
            .unwrap_or_default();

    Ok(DatabaseTableKey {
        name: constraint_name,
        constraint_type: constraint_type.clone(),
        column_names: split_database_object_names(&column_names),
        detail: if constraint_definition.is_empty() {
            constraint_type
        } else {
            constraint_definition
        },
    })
}

fn describe_postgres_index(row: &PgRow) -> Result<DatabaseTableIndex> {
    let index_name = decode_postgres_text_column_at(row, 0, "index name")?;
    let index_definition =
        decode_postgres_text_column_at(row, 1, &format!("index definition for {index_name}"))?;

    Ok(DatabaseTableIndex {
        name: index_name,
        detail: index_definition,
    })
}

fn format_database_table_key_detail(
    constraint_type: &str,
    column_names: &str,
    referenced_table_name: Option<&str>,
    referenced_column_names: Option<&str>,
) -> String {
    let column_segment = if column_names.trim().is_empty() {
        "(unknown columns)".to_string()
    } else {
        format!("({})", column_names.trim())
    };
    let mut detail = format!("{} {column_segment}", constraint_type.trim());
    if constraint_type.eq_ignore_ascii_case("FOREIGN KEY") {
        if let Some(referenced_table_name) = referenced_table_name
            .map(str::trim)
            .filter(|referenced_table_name| !referenced_table_name.is_empty())
        {
            detail.push_str(" -> ");
            detail.push_str(referenced_table_name);
            if let Some(referenced_column_names) = referenced_column_names
                .map(str::trim)
                .filter(|referenced_column_names| !referenced_column_names.is_empty())
            {
                detail.push(' ');
                detail.push('(');
                detail.push_str(referenced_column_names);
                detail.push(')');
            }
        }
    }
    detail
}

fn split_database_object_names(names: &str) -> Vec<String> {
    names
        .split(',')
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn format_mysql_column_key(column_key: &str) -> Option<&'static str> {
    match column_key {
        "PRI" => Some("PRIMARY KEY"),
        "UNI" => Some("UNIQUE"),
        "MUL" => Some("INDEX"),
        _ if column_key.is_empty() => None,
        _ => Some("KEY"),
    }
}

fn format_mysql_qualified_name(schema_name: &str, table_name: &str) -> String {
    format!(
        "{}.{}",
        quote_mysql_identifier(schema_name),
        quote_mysql_identifier(table_name)
    )
}

fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_mysql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn validate_mysql_token(label: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{label} is required."));
    }

    if value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        Ok(())
    } else {
        Err(anyhow!(
            "{label} can only contain letters, numbers, and underscores."
        ))
    }
}

fn build_connection_url(connection_form: &DatabaseConnectionForm) -> Result<(String, Url)> {
    if connection_form.host.is_empty() {
        return Err(anyhow!("Host is required."));
    }

    if connection_form.user.is_empty() {
        return Err(anyhow!("User is required."));
    }

    let port = if connection_form.port.is_empty() {
        connection_form.kind.default_port()
    } else {
        let port = connection_form
            .port
            .parse::<u16>()
            .context("Port must be a number between 1 and 65535.")?;
        if port == 0 {
            return Err(anyhow!("Port must be a number between 1 and 65535."));
        }
        port
    };

    let mut connection_url = format!(
        "{}://{}",
        connection_form.kind.normalized_scheme(),
        encode_connection_component(&connection_form.user)
    );

    if !connection_form.password.is_empty() {
        connection_url.push(':');
        connection_url.push_str(&encode_connection_component(&connection_form.password));
    }

    connection_url.push('@');
    connection_url.push_str(&connection_form.host);
    connection_url.push(':');
    connection_url.push_str(&port.to_string());

    if !connection_form.database_name.is_empty() {
        connection_url.push('/');
        connection_url.push_str(&encode_connection_component(&connection_form.database_name));
    }

    if !connection_form.options.is_empty() {
        connection_url.push('?');
        connection_url.push_str(connection_form.options.trim_start_matches('?'));
    }

    let parsed_url =
        Url::parse(&connection_url).context("Connection details produced an invalid URL.")?;
    Ok((connection_url, parsed_url))
}

fn normalize_saved_connection(
    saved_connection: SavedDatabaseConnection,
) -> Result<SavedDatabaseConnection> {
    let connection_form = connection_form_from_saved_connection(&saved_connection)?;
    let (normalized_url, _) = build_connection_url(&connection_form)?;
    Ok(SavedDatabaseConnection {
        url: normalized_url,
        kind: connection_form.kind,
        ..saved_connection
    })
}

fn connection_form_from_saved_connection(
    saved_connection: &SavedDatabaseConnection,
) -> Result<DatabaseConnectionForm> {
    let parsed_url =
        Url::parse(&saved_connection.url).context("Could not parse the saved connection URL.")?;
    let host = parsed_url
        .host_str()
        .context("Saved connection is missing a host.")?
        .to_string();

    Ok(DatabaseConnectionForm {
        kind: DatabaseKind::from_scheme(parsed_url.scheme()).unwrap_or(saved_connection.kind),
        host,
        port: parsed_url
            .port()
            .map(|port| port.to_string())
            .unwrap_or_default(),
        user: decode_connection_component(parsed_url.username()),
        password: parsed_url
            .password()
            .map(decode_connection_component)
            .unwrap_or_default(),
        database_name: decode_connection_component(parsed_url.path().trim_start_matches('/')),
        options: parsed_url.query().unwrap_or_default().to_string(),
    })
}

fn render_database_input<T>(editor: Entity<Editor>, cx: &mut Context<T>) -> impl IntoElement {
    h_flex()
        .w_full()
        .h(px(34.))
        .px_2()
        .items_center()
        .rounded_md()
        .border_1()
        .border_color(cx.theme().colors().border)
        .bg(cx.theme().colors().editor_background)
        .child(editor)
}

fn render_database_multiline_input<T>(
    editor: Entity<Editor>,
    height: Pixels,
    cx: &mut Context<T>,
) -> impl IntoElement {
    v_flex()
        .w_full()
        .min_h(height)
        .max_h(height)
        .px_2()
        .py_2()
        .rounded_md()
        .border_1()
        .border_color(cx.theme().colors().border)
        .bg(cx.theme().colors().editor_background)
        .child(editor)
}

fn render_database_field<T>(
    label: &'static str,
    editor: Entity<Editor>,
    cx: &mut Context<T>,
) -> impl IntoElement {
    v_flex()
        .w_full()
        .gap_1()
        .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
        .child(render_database_input(editor, cx))
}

fn render_database_read_only_field<T>(
    label: &'static str,
    value: impl Into<String>,
    cx: &mut Context<T>,
) -> impl IntoElement {
    v_flex()
        .w_full()
        .gap_1()
        .child(Label::new(label).size(LabelSize::Small).color(Color::Muted))
        .child(
            h_flex()
                .w_full()
                .min_h(px(34.))
                .px_2()
                .py_1p5()
                .items_center()
                .rounded_md()
                .border_1()
                .border_color(cx.theme().colors().border)
                .bg(cx.theme().colors().editor_background)
                .child(
                    Label::new(value.into())
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                ),
        )
}

fn push_disabled_context_menu_entry(menu: ContextMenu, label: &'static str) -> ContextMenu {
    let mut menu = menu;
    menu.push_item(ContextMenuEntry::new(label).disabled(true));
    menu
}

fn copy_database_panel_value(
    panel: Entity<DatabasePanel>,
    label: &'static str,
    value: String,
    cx: &mut App,
) {
    cx.write_to_clipboard(ClipboardItem::new_string(value));
    panel.update(cx, |this, cx| {
        this.notice = Some(PanelNotice {
            kind: NoticeKind::Info,
            message: format!("Copied {label} to clipboard."),
        });
        cx.notify();
    });
}

fn encode_connection_component(component: &str) -> String {
    urlencoding::encode(component).into_owned()
}

fn decode_connection_component(component: &str) -> String {
    match urlencoding::decode(component) {
        Ok(decoded_component) => decoded_component.into_owned(),
        Err(_) => component.to_string(),
    }
}

fn selected_mysql_schema_name(connection_url: &str) -> Result<Option<String>> {
    let parsed_url =
        Url::parse(connection_url).context("Could not parse the MySQL connection URL.")?;
    let schema_name = decode_connection_component(parsed_url.path().trim_start_matches('/'));
    if schema_name.is_empty() {
        Ok(None)
    } else {
        Ok(Some(schema_name))
    }
}

fn quote_mysql_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}

fn set_editor_text(editor: &Entity<Editor>, text: String, cx: &mut App) {
    editor.update(cx, move |editor, cx| {
        if let Some(buffer) = editor.buffer().read(cx).as_singleton() {
            buffer.update(cx, |buffer, cx| {
                buffer.set_text(text, cx);
            });
        }
    });
}

fn decode_mysql_text_column(row: &MySqlRow, label: &str) -> Result<String> {
    decode_mysql_text_column_at(row, 0, label)
}

fn decode_mysql_text_column_at(row: &MySqlRow, index: usize, label: &str) -> Result<String> {
    match row.try_get::<String, _>(index) {
        Ok(value) => Ok(value),
        Err(string_error) => {
            let bytes = row.try_get::<Vec<u8>, _>(index).with_context(|| {
                format!(
                    "Could not decode {label} from column {index} as bytes after String decoding failed: {string_error}"
                )
            })?;
            String::from_utf8(bytes).with_context(|| {
                format!(
                    "Could not decode {label} from column {index} as UTF-8 after String decoding failed: {string_error}"
                )
            })
        }
    }
}

fn decode_mysql_optional_text_column_at(
    row: &MySqlRow,
    index: usize,
    label: &str,
) -> Result<Option<String>> {
    match row.try_get::<Option<String>, _>(index) {
        Ok(value) => Ok(value),
        Err(string_error) => {
            let bytes = row.try_get::<Option<Vec<u8>>, _>(index).with_context(|| {
                format!(
                    "Could not decode {label} from column {index} as bytes after String decoding failed: {string_error}"
                )
            })?;

            bytes.map(|bytes| {
                String::from_utf8(bytes).with_context(|| {
                    format!(
                        "Could not decode {label} from column {index} as UTF-8 after String decoding failed: {string_error}"
                    )
                })
            })
            .transpose()
        }
    }
}

fn decode_postgres_text_column_at(row: &PgRow, index: usize, label: &str) -> Result<String> {
    row.try_get::<String, _>(index)
        .with_context(|| format!("Could not decode {label} from column {index}"))
}

fn decode_postgres_optional_text_column_at(
    row: &PgRow,
    index: usize,
    label: &str,
) -> Result<Option<String>> {
    row.try_get::<Option<String>, _>(index)
        .with_context(|| format!("Could not decode {label} from column {index}"))
}

fn format_connection_error(error: &anyhow::Error, connection_url: &str) -> String {
    let mut error_messages = Vec::new();
    for chain_error in error.chain() {
        let error_message = sanitize_connection_error(&chain_error.to_string(), connection_url);
        if error_messages
            .last()
            .is_none_or(|previous_error_message| previous_error_message != &error_message)
        {
            error_messages.push(error_message);
        }
    }

    error_messages.join(": ")
}

fn sanitize_connection_error(error_message: &str, connection_url: &str) -> String {
    error_message.replace(connection_url, "<connection-url>")
}

fn total_table_count(metadata: &DatabaseMetadata) -> usize {
    metadata
        .schemas
        .iter()
        .map(|schema| schema.tables.len())
        .sum()
}

pub struct DatabaseQueryConsole {
    connection: SavedDatabaseConnection,
    default_schema_name: Option<String>,
    target_table_path: Option<DatabaseTablePath>,
    query_editor: Entity<Editor>,
    output_editor: Entity<Editor>,
    result_table: Option<QueryConsoleResultTable>,
    result_table_column_widths: Option<Entity<RedistributableColumnsState>>,
    result_table_scroll_handle: ScrollHandle,
    selected_row_index: Option<usize>,
    selected_row_editors: Vec<Entity<Editor>>,
    insert_row_editors: Option<Vec<Entity<Editor>>>,
    results_panel_height: Pixels,
    split_container_height: Pixels,
    summary_expanded: bool,
    status: QueryConsoleStatus,
    status_message: String,
    status_kind: NoticeKind,
    running_task: Option<Task<()>>,
}

pub struct DatabaseQueryConsoleToolbarItemView {
    query_console: Option<Entity<DatabaseQueryConsole>>,
    query_console_subscription: Option<Subscription>,
}

impl DatabaseQueryConsole {
    fn new(
        connection: SavedDatabaseConnection,
        default_schema_name: Option<String>,
        target_table_path: Option<DatabaseTablePath>,
        initial_text: Option<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let placeholder_text =
            query_console_placeholder_text(connection.kind, default_schema_name.as_deref());
        let query_editor = cx.new(|cx| {
            let mut editor = Editor::multi_line(window, cx);
            editor.set_mode(EditorMode::Full {
                scale_ui_elements_with_buffer_font_size: true,
                show_active_line_background: true,
                sizing_behavior: SizingBehavior::ExcludeOverscrollMargin,
            });
            editor.set_placeholder_text(&placeholder_text, window, cx);
            editor.hide_minimap_by_default(window, cx);
            editor.set_show_git_diff_gutter(false, cx);
            editor.set_show_runnables(false, cx);
            editor.set_show_breakpoints(false, cx);
            editor.set_show_edit_predictions(Some(false), window, cx);
            if let Some(initial_text) = initial_text.clone() {
                editor.set_text(initial_text, window, cx);
            }
            editor
        });
        let output_editor = cx.new(|cx| {
            let mut editor = Editor::multi_line(window, cx);
            editor.set_mode(EditorMode::Full {
                scale_ui_elements_with_buffer_font_size: true,
                show_active_line_background: true,
                sizing_behavior: SizingBehavior::ExcludeOverscrollMargin,
            });
            editor.hide_minimap_by_default(window, cx);
            editor.set_show_gutter(false, cx);
            editor.set_show_git_diff_gutter(false, cx);
            editor.set_show_runnables(false, cx);
            editor.set_show_breakpoints(false, cx);
            editor.set_show_edit_predictions(Some(false), window, cx);
            editor.set_read_only(true);
            editor.set_text(QUERY_CONSOLE_EMPTY_RESULTS, window, cx);
            editor
        });
        let ui_state = read_query_console_ui_state(cx);

        Self {
            connection,
            default_schema_name: default_schema_name.clone(),
            target_table_path,
            query_editor,
            output_editor,
            result_table: None,
            result_table_column_widths: None,
            result_table_scroll_handle: ScrollHandle::new(),
            selected_row_index: None,
            selected_row_editors: Vec::new(),
            insert_row_editors: None,
            results_panel_height: normalize_query_console_results_panel_height(px(
                ui_state.results_panel_height
            )),
            split_container_height: Pixels::ZERO,
            summary_expanded: ui_state.summary_expanded,
            status: QueryConsoleStatus::Idle,
            status_message: query_console_ready_message(default_schema_name.as_deref()),
            status_kind: NoticeKind::Info,
            running_task: None,
        }
    }

    fn set_result_table(
        &mut self,
        result_table: Option<QueryConsoleResultTable>,
        cx: &mut Context<Self>,
    ) {
        self.result_table = result_table;
        self.result_table_column_widths = self
            .result_table
            .as_ref()
            .map(|result_table| create_query_console_result_table_column_widths(result_table, cx));
        self.selected_row_index = None;
        self.selected_row_editors.clear();
        self.insert_row_editors = None;
    }

    fn focus_selected_row_editor(
        &self,
        column_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if let Some(editor) = self.selected_row_editors.get(column_index) {
            editor.focus_handle(cx).focus(window, cx);
        } else if let Some(editor) = self.selected_row_editors.first() {
            editor.focus_handle(cx).focus(window, cx);
        }
    }

    fn run_queries(
        &mut self,
        run_target: QueryConsoleRunTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if matches!(self.status, QueryConsoleStatus::Running) {
            return;
        }

        let statement_sqls = match self.prepare_run_statements(run_target, window, cx) {
            Ok(statement_sqls) => statement_sqls,
            Err(error) => {
                self.status = QueryConsoleStatus::Idle;
                self.status_kind = NoticeKind::Error;
                self.status_message = error.to_string();
                set_editor_text(&self.output_editor, error.to_string(), cx);
                cx.notify();
                return;
            }
        };

        let statement_count = statement_sqls.len();
        let connection_kind = self.connection.kind;
        let default_schema_name = self.default_schema_name.clone();
        let connection_url = self.connection.url.clone();
        let connection_url_for_task = connection_url.clone();
        let target_table_path = self.target_table_path.clone();
        let runtime_handle = Tokio::handle(cx);

        self.status = QueryConsoleStatus::Running;
        self.status_kind = NoticeKind::Info;
        self.status_message = if statement_count == 1 {
            "Running 1 statement...".to_string()
        } else {
            format!("Running {statement_count} statements...")
        };
        self.set_result_table(None, cx);
        set_editor_text(&self.output_editor, "Running SQL...".to_string(), cx);
        cx.notify();

        let task = cx.spawn(async move |this, cx| {
            let execution = cx
                .background_spawn(async move {
                    let worker = std::thread::Builder::new()
                        .name("query-console-execution".to_string())
                        .spawn(move || {
                            runtime_handle.block_on(async move {
                                execute_query_console_statements(
                                    connection_kind,
                                    connection_url_for_task,
                                    default_schema_name,
                                    target_table_path,
                                    statement_sqls,
                                )
                                .await
                            })
                        })
                        .context("Could not spawn query console worker thread")?;

                    match worker.join() {
                        Ok(result) => result,
                        Err(_) => Err(anyhow!("Query console worker thread panicked")),
                    }
                })
                .await;

            let update_result = this.update(cx, |this, cx| {
                this.status = QueryConsoleStatus::Idle;
                match execution {
                    Ok(summary) => {
                        this.status_kind = if summary.error_message.is_some() {
                            NoticeKind::Error
                        } else {
                            NoticeKind::Success
                        };
                        this.status_message = match summary.stopped_at_statement_number {
                            Some(statement_number) => {
                                if let Some(error_message) = &summary.error_message {
                                    format!(
                                        "Stopped at statement {statement_number}. {error_message}"
                                    )
                                } else {
                                    format!("Stopped at statement {statement_number}.")
                                }
                            }
                            None if summary.completed_statement_count == 1 => {
                                "Completed 1 statement.".to_string()
                            }
                            None => {
                                format!(
                                    "Completed {} statements.",
                                    summary.completed_statement_count
                                )
                            }
                        };
                        this.set_result_table(summary.result_table, cx);
                        set_editor_text(&this.output_editor, summary.output_text, cx);
                    }
                    Err(error) => {
                        let error_message = format_connection_error(&error, &connection_url);
                        this.status_kind = NoticeKind::Error;
                        this.status_message = format!("Execution failed. {error_message}");
                        this.set_result_table(None, cx);
                        set_editor_text(
                            &this.output_editor,
                            format!("Could not execute SQL.\n\n{error_message}"),
                            cx,
                        );
                    }
                }
                cx.notify();
            });

            if let Err(error) = update_result {
                log::debug!("Database query console closed before completion: {error:#}");
            }
        });
        self.running_task = Some(task);
    }

    fn prepare_run_statements(
        &self,
        run_target: QueryConsoleRunTarget,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Result<Vec<String>> {
        let query_text = self.query_editor.read(cx).text(cx);
        let selection_range = self.query_editor.update(cx, |editor, cx| {
            editor
                .selected_text_range(true, window, cx)
                .map(|selection| selection.range)
        });

        match run_target {
            QueryConsoleRunTarget::All => {
                let statements = split_query_console_statements(&query_text);
                if statements.is_empty() {
                    Err(anyhow!(
                        "The query console does not contain any SQL statements."
                    ))
                } else {
                    Ok(statements
                        .into_iter()
                        .map(|statement| statement.sql)
                        .collect())
                }
            }
            QueryConsoleRunTarget::SelectionOrCurrent => {
                if let Some(selection_range) = selection_range
                    .clone()
                    .filter(|selection_range| !selection_range.is_empty())
                {
                    let selected_text = utf16_range_to_string_slice(&query_text, selection_range)
                        .context("Could not read the selected SQL.")?;
                    let statements = split_query_console_statements(selected_text);
                    if statements.is_empty() {
                        Err(anyhow!(
                            "The current selection does not contain any SQL statements."
                        ))
                    } else {
                        Ok(statements
                            .into_iter()
                            .map(|statement| statement.sql)
                            .collect())
                    }
                } else {
                    let statements = split_query_console_statements(&query_text);
                    if statements.is_empty() {
                        return Err(anyhow!(
                            "The query console does not contain any SQL statements."
                        ));
                    }

                    let cursor_utf16 = selection_range.map_or(0, |selection_range| {
                        selection_range.start.min(selection_range.end)
                    });
                    let statement = current_query_console_statement(&statements, cursor_utf16)
                        .context("Could not find a SQL statement at the current cursor.")?;
                    Ok(vec![statement.sql.clone()])
                }
            }
        }
    }

    fn clear_results(&mut self, cx: &mut Context<Self>) {
        if matches!(self.status, QueryConsoleStatus::Running) {
            return;
        }

        self.status = QueryConsoleStatus::Idle;
        self.status_kind = NoticeKind::Info;
        self.status_message = query_console_ready_message(self.default_schema_name.as_deref());
        self.set_result_table(None, cx);
        set_editor_text(
            &self.output_editor,
            QUERY_CONSOLE_EMPTY_RESULTS.to_string(),
            cx,
        );
        cx.notify();
    }

    fn select_result_row(
        &mut self,
        row_index: usize,
        focus_column_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(result_table) = &self.result_table else {
            return;
        };
        if !result_table.supports_update_delete() {
            return;
        }
        if self.selected_row_index == Some(row_index) && !self.selected_row_editors.is_empty() {
            self.focus_selected_row_editor(focus_column_index, window, cx);
            return;
        }

        let Some(row) = result_table.rows.get(row_index) else {
            return;
        };

        self.selected_row_index = Some(row_index);
        self.selected_row_editors = create_query_console_row_editors(
            self.connection.kind,
            &result_table.column_names,
            Some(row),
            window,
            cx,
        );
        self.insert_row_editors = None;
        self.focus_selected_row_editor(focus_column_index, window, cx);
        cx.notify();
    }

    fn clear_selected_row(&mut self, cx: &mut Context<Self>) {
        self.selected_row_index = None;
        self.selected_row_editors.clear();
        cx.notify();
    }

    fn start_insert_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(result_table) = &self.result_table else {
            return;
        };
        if !result_table.supports_insert() {
            return;
        }

        self.selected_row_index = None;
        self.selected_row_editors.clear();
        self.insert_row_editors = Some(create_query_console_row_editors(
            self.connection.kind,
            &result_table.column_names,
            None,
            window,
            cx,
        ));
        if let Some(first_editor) = self
            .insert_row_editors
            .as_ref()
            .and_then(|editors| editors.first())
        {
            first_editor.focus_handle(cx).focus(window, cx);
        }
        cx.notify();
    }

    fn cancel_insert_row(&mut self, cx: &mut Context<Self>) {
        self.insert_row_editors = None;
        cx.notify();
    }

    fn append_generated_sql(
        &mut self,
        statement_sql: &str,
        success_message: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let existing_text = self.query_editor.read(cx).text(cx);
        let new_text = append_query_console_statement_text(&existing_text, statement_sql);
        self.query_editor.update(cx, |editor, cx| {
            editor.set_text(new_text, window, cx);
        });
        self.status = QueryConsoleStatus::Idle;
        self.status_kind = NoticeKind::Success;
        self.status_message = success_message;
        self.query_editor.focus_handle(cx).focus(window, cx);
        cx.notify();
    }

    fn generate_update_for_selected_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(result_table) = &self.result_table else {
            return;
        };
        let Some(row_index) = self.selected_row_index else {
            self.status_kind = NoticeKind::Error;
            self.status_message = "Select a row first.".to_string();
            cx.notify();
            return;
        };
        let Some(original_row) = result_table.rows.get(row_index).cloned() else {
            self.status_kind = NoticeKind::Error;
            self.status_message = "The selected row is no longer available.".to_string();
            cx.notify();
            return;
        };
        let edited_values = read_query_console_row_editor_values(&self.selected_row_editors, cx);
        match build_query_console_update_statement(
            self.connection.kind,
            result_table,
            &original_row,
            &edited_values,
        ) {
            Ok(statement_sql) => {
                if let Some(result_table) = self.result_table.as_mut() {
                    query_console_apply_local_row_update(
                        self.connection.kind,
                        result_table,
                        row_index,
                        &edited_values,
                        &original_row,
                    );
                }
                self.clear_selected_row(cx);
                self.append_generated_sql(
                    &statement_sql,
                    format!(
                        "UPDATE for row {} was added to the SQL editor. Run it to persist the change.",
                        row_index + 1
                    ),
                    window,
                    cx,
                );
            }
            Err(error) => {
                self.status_kind = NoticeKind::Error;
                self.status_message = error.to_string();
                cx.notify();
            }
        }
    }

    fn generate_delete_for_row(
        &mut self,
        row_index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(result_table) = &self.result_table else {
            return;
        };
        let Some(original_row) = result_table.rows.get(row_index).cloned() else {
            self.status_kind = NoticeKind::Error;
            self.status_message = "The selected row is no longer available.".to_string();
            cx.notify();
            return;
        };
        match build_query_console_delete_statement(
            self.connection.kind,
            result_table,
            &original_row,
        ) {
            Ok(statement_sql) => {
                if let Some(result_table) = self.result_table.as_mut() {
                    query_console_apply_local_row_delete(result_table, row_index);
                }
                if self.selected_row_index == Some(row_index) {
                    self.clear_selected_row(cx);
                }
                self.append_generated_sql(
                    &statement_sql,
                    format!(
                        "DELETE for row {} was added to the SQL editor. Run it to persist the change.",
                        row_index + 1
                    ),
                    window,
                    cx,
                );
            }
            Err(error) => {
                self.status_kind = NoticeKind::Error;
                self.status_message = error.to_string();
                cx.notify();
            }
        }
    }

    fn generate_insert_from_draft_row(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(result_table) = &self.result_table else {
            return;
        };
        let Some(insert_row_editors) = &self.insert_row_editors else {
            self.status_kind = NoticeKind::Error;
            self.status_message = "Add a row first.".to_string();
            cx.notify();
            return;
        };

        let edited_values = read_query_console_row_editor_values(insert_row_editors, cx);
        match build_query_console_insert_statement(
            self.connection.kind,
            result_table,
            &edited_values,
        ) {
            Ok(statement_sql) => {
                if let Some(result_table) = self.result_table.as_mut() {
                    query_console_apply_local_row_insert(
                        self.connection.kind,
                        result_table,
                        &edited_values,
                    );
                }
                self.append_generated_sql(
                    &statement_sql,
                    "INSERT was added to the SQL editor. Run it to persist the new row."
                        .to_string(),
                    window,
                    cx,
                );
                self.insert_row_editors = None;
            }
            Err(error) => {
                self.status_kind = NoticeKind::Error;
                self.status_message = error.to_string();
                cx.notify();
            }
        }
    }

    fn toggle_summary_expanded(&mut self, cx: &mut Context<Self>) {
        self.summary_expanded = !self.summary_expanded;
        self.persist_ui_state(cx);
        cx.notify();
    }

    fn update_split_container_height(
        &mut self,
        split_container_height: Pixels,
        cx: &mut Context<Self>,
    ) {
        if self.split_container_height == split_container_height {
            return;
        }

        self.split_container_height = split_container_height;
        let clamped_height = clamp_query_console_results_panel_height(
            self.results_panel_height,
            split_container_height,
        );
        if self.results_panel_height != clamped_height {
            self.results_panel_height = clamped_height;
            cx.notify();
        }
    }

    fn resize_results_panel(
        &mut self,
        desired_results_panel_height: Pixels,
        available_height: Pixels,
        cx: &mut Context<Self>,
    ) {
        let clamped_height = clamp_query_console_results_panel_height(
            desired_results_panel_height,
            available_height,
        );
        if self.results_panel_height != clamped_height {
            self.results_panel_height = clamped_height;
            cx.notify();
        }
    }

    fn reset_results_panel_height(&mut self, cx: &mut Context<Self>) {
        let default_height = clamp_query_console_results_panel_height(
            QUERY_CONSOLE_DEFAULT_RESULTS_PANEL_HEIGHT,
            self.effective_split_container_height(),
        );
        if self.results_panel_height != default_height {
            self.results_panel_height = default_height;
            cx.notify();
        }
        self.persist_ui_state(cx);
    }

    fn effective_split_container_height(&self) -> Pixels {
        if self.split_container_height > Pixels::ZERO {
            self.split_container_height
        } else {
            QUERY_CONSOLE_DEFAULT_RESULTS_PANEL_HEIGHT
                + QUERY_CONSOLE_MIN_SQL_PANEL_HEIGHT
                + QUERY_CONSOLE_RESULTS_RESIZE_HANDLE_SIZE
        }
    }

    fn persist_ui_state(&self, cx: &App) {
        let kvp = KeyValueStore::global(cx);
        let ui_state = SerializedQueryConsoleUiState {
            results_panel_height: f32::from(self.results_panel_height),
            summary_expanded: self.summary_expanded,
        };
        db::write_and_log(cx, move || async move {
            let json = serde_json::to_string(&ui_state)?;
            kvp.scoped(DATABASE_PANEL_KEY)
                .write(QUERY_CONSOLE_UI_STATE_KEY.to_string(), json)
                .await
        });
    }

    fn render_results_resize_handle(&self, cx: &mut Context<Self>) -> AnyElement {
        h_flex()
            .id("database-query-console-results-resize-handle")
            .w_full()
            .h(QUERY_CONSOLE_RESULTS_RESIZE_HANDLE_SIZE)
            .flex_shrink_0()
            .items_center()
            .cursor_row_resize()
            .hover(|style| style.bg(cx.theme().colors().element_hover))
            .on_drag(
                DraggedQueryConsoleResultsResizeHandle,
                |dragged, _, _, cx| {
                    cx.stop_propagation();
                    cx.new(|_| dragged.clone())
                },
            )
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .on_mouse_up(
                MouseButton::Left,
                cx.listener(|this, event: &MouseUpEvent, _, cx| {
                    if event.click_count == 2 {
                        this.reset_results_panel_height(cx);
                    } else {
                        this.persist_ui_state(cx);
                    }
                    cx.stop_propagation();
                }),
            )
            .child(div().w_full().h(px(1.)).bg(cx.theme().colors().border))
            .into_any_element()
    }

    fn render_inline_insert_actions(&self, cx: &mut Context<Self>) -> AnyElement {
        h_flex()
            .gap_1()
            .items_center()
            .child(
                IconButton::new("database-query-console-append-insert", IconName::Check)
                    .shape(IconButtonShape::Square)
                    .style(ButtonStyle::Tinted(TintColor::Success))
                    .icon_size(IconSize::Small)
                    .disabled(matches!(self.status, QueryConsoleStatus::Running))
                    .tooltip(Tooltip::text("Add INSERT to the SQL editor"))
                    .on_click(cx.listener(|this, _, window, cx| {
                        this.generate_insert_from_draft_row(window, cx);
                    })),
            )
            .child(
                IconButton::new("database-query-console-cancel-insert", IconName::Close)
                    .shape(IconButtonShape::Square)
                    .style(ButtonStyle::Subtle)
                    .icon_size(IconSize::Small)
                    .disabled(matches!(self.status, QueryConsoleStatus::Running))
                    .tooltip(Tooltip::text("Cancel row insert"))
                    .on_click(cx.listener(|this, _, _, cx| {
                        this.cancel_insert_row(cx);
                    })),
            )
            .into_any_element()
    }

    fn render_inline_update_actions(&self, row_index: usize, cx: &mut Context<Self>) -> AnyElement {
        h_flex()
            .gap_1()
            .items_center()
            .child(
                IconButton::new(
                    ("database-query-console-append-update", row_index),
                    IconName::Check,
                )
                .shape(IconButtonShape::Square)
                .style(ButtonStyle::Tinted(TintColor::Success))
                .icon_size(IconSize::Small)
                .disabled(matches!(self.status, QueryConsoleStatus::Running))
                .tooltip(Tooltip::text("Add UPDATE to the SQL editor"))
                .on_click(cx.listener(|this, _, window, cx| {
                    this.generate_update_for_selected_row(window, cx);
                })),
            )
            .child(
                IconButton::new(
                    ("database-query-console-cancel-edit-row", row_index),
                    IconName::Close,
                )
                .shape(IconButtonShape::Square)
                .style(ButtonStyle::Subtle)
                .icon_size(IconSize::Small)
                .disabled(matches!(self.status, QueryConsoleStatus::Running))
                .tooltip(Tooltip::text("Cancel row editing"))
                .on_click(cx.listener(|this, _, _, cx| {
                    this.clear_selected_row(cx);
                })),
            )
            .into_any_element()
    }

    fn render_inline_delete_action(&self, row_index: usize, cx: &mut Context<Self>) -> AnyElement {
        IconButton::new(
            ("database-query-console-delete-row", row_index),
            IconName::Trash,
        )
        .shape(IconButtonShape::Square)
        .style(ButtonStyle::Tinted(TintColor::Error))
        .icon_size(IconSize::Small)
        .disabled(matches!(self.status, QueryConsoleStatus::Running))
        .tooltip(Tooltip::text("Add DELETE to the SQL editor"))
        .on_click(cx.listener(move |this, _, window, cx| {
            this.generate_delete_for_row(row_index, window, cx);
        }))
        .into_any_element()
    }

    fn render_row_editor_cell_with_actions(
        &self,
        editor: Entity<Editor>,
        actions: AnyElement,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        v_flex()
            .w_full()
            .min_w(px(0.))
            .gap_1()
            .child(render_database_input(editor, cx))
            .child(h_flex().w_full().justify_end().child(actions))
            .into_any_element()
    }

    fn render_results_panel(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let Some(result_table) = &self.result_table else {
            return render_database_fill_editor(self.output_editor.clone(), cx).into_any_element();
        };

        let result_summary = query_console_result_table_summary(result_table);
        let mutation_message = result_table.mutation_hint_message();

        v_flex()
            .size_full()
            .gap_2()
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .items_center()
                    .gap_2()
                    .child(
                        Label::new(result_summary)
                            .size(LabelSize::Small)
                            .color(Color::Muted),
                    )
                    .child(
                        h_flex()
                            .gap_2()
                            .when(result_table.supports_insert(), |this| {
                                this.child(
                                    Button::new("database-query-console-add-row", "Add Row")
                                        .size(ButtonSize::Compact)
                                        .style(ButtonStyle::Subtle)
                                        .disabled(matches!(
                                            self.status,
                                            QueryConsoleStatus::Running
                                        ))
                                        .on_click(cx.listener(|this, _, window, cx| {
                                            this.start_insert_row(window, cx);
                                        })),
                                )
                            })
                            .when(self.selected_row_index.is_some(), |this| {
                                this.child(
                                    Button::new(
                                        "database-query-console-clear-selection",
                                        "Cancel Edit",
                                    )
                                    .size(ButtonSize::Compact)
                                    .style(ButtonStyle::Subtle)
                                    .disabled(matches!(self.status, QueryConsoleStatus::Running))
                                    .on_click(cx.listener(
                                        |this, _, _, cx| {
                                            this.clear_selected_row(cx);
                                        },
                                    )),
                                )
                            }),
                    ),
            )
            .when_some(mutation_message, |this, mutation_message| {
                this.child(
                    Label::new(mutation_message)
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
            })
            .child(self.render_result_table_view(window, cx))
            .child(
                v_flex()
                    .gap_1()
                    .child(
                        h_flex()
                            .w_full()
                            .items_center()
                            .gap_1()
                            .child(
                                Disclosure::new(
                                    "database-query-console-summary-disclosure",
                                    self.summary_expanded,
                                )
                                .on_click(cx.listener(
                                    |this, _, _, cx| {
                                        this.toggle_summary_expanded(cx);
                                    },
                                )),
                            )
                            .child(
                                Label::new("Summary")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            ),
                    )
                    .when(self.summary_expanded, |this| {
                        this.child(
                            v_flex()
                                .h(px(96.))
                                .min_h(px(72.))
                                .child(render_database_fill_editor(self.output_editor.clone(), cx)),
                        )
                    }),
            )
            .into_any_element()
    }

    fn render_result_table_view(&self, window: &mut Window, cx: &mut Context<Self>) -> AnyElement {
        let Some(result_table) = &self.result_table else {
            return div().into_any_element();
        };
        let Some(column_widths) = self.result_table_column_widths.clone() else {
            return div().into_any_element();
        };

        let query_console = cx.entity();
        let query_console_entity_id = cx.entity_id();
        let supports_update_delete = result_table.supports_update_delete();
        let insert_row_offset = usize::from(self.insert_row_editors.is_some());
        let editing_display_row_index = self
            .selected_row_index
            .map(|row_index| row_index + insert_row_offset);
        let total_columns = result_table.column_names.len();
        let total_display_rows = result_table.rows.len() + insert_row_offset;
        let rendered_column_widths = column_widths.read(cx).widths_to_render().into_vec();
        let table_width = query_console_result_table_width(result_table);
        let selected_row_background = cx.theme().colors().element_selected;
        let striped_row_background = cx.theme().colors().text.opacity(0.05);
        let hover_row_background = cx.theme().colors().element_hover.opacity(0.6);
        let table_border_color = cx.theme().colors().border;
        let render_table_cell = |width, content: AnyElement| {
            div()
                .w(width)
                .min_w(width)
                .px_1()
                .py_0p5()
                .overflow_hidden()
                .child(content)
                .into_any_element()
        };
        let render_table_row = |display_row_index: usize, items: Vec<AnyElement>| {
            let is_highlighted = editing_display_row_index == Some(display_row_index)
                || (insert_row_offset > 0 && display_row_index == 0);
            let background = if is_highlighted {
                Some(selected_row_background)
            } else if display_row_index % 2 == 1 {
                Some(striped_row_background)
            } else {
                None
            };

            let mut row = h_flex()
                .w_full()
                .when_some(background, |this, background| this.bg(background))
                .when(!is_highlighted, |this| {
                    this.hover(|row| row.bg(hover_row_background))
                })
                .when(display_row_index + 1 < total_display_rows, |this| {
                    this.border_b_1().border_color(table_border_color)
                });

            for (item, width) in items
                .into_iter()
                .zip(rendered_column_widths.iter().cloned())
            {
                row = row.child(render_table_cell(width, item));
            }

            row.into_any_element()
        };
        let header = h_flex()
            .w_full()
            .border_b_1()
            .border_color(table_border_color)
            .children(
                result_table
                    .column_names
                    .iter()
                    .cloned()
                    .zip(rendered_column_widths.iter().cloned())
                    .map(|(column_name, width)| {
                        render_table_cell(
                            width,
                            Label::new(column_name)
                                .size(LabelSize::Small)
                                .color(Color::Muted)
                                .into_any_element(),
                        )
                    }),
            )
            .into_any_element();
        let mut rendered_rows = Vec::with_capacity(total_display_rows);

        if let Some(insert_row_editors) = &self.insert_row_editors {
            let last_column_index = insert_row_editors.len().saturating_sub(1);
            let mut items = Vec::with_capacity(insert_row_editors.len());
            for (column_index, editor) in insert_row_editors.iter().enumerate() {
                if column_index == last_column_index {
                    items.push(self.render_row_editor_cell_with_actions(
                        editor.clone(),
                        self.render_inline_insert_actions(cx),
                        cx,
                    ));
                } else {
                    items.push(render_database_input(editor.clone(), cx).into_any_element());
                }
            }
            rendered_rows.push(render_table_row(0, items));
        }

        for (row_index, row) in result_table.rows.iter().enumerate() {
            let is_editing_row = self.selected_row_index == Some(row_index);
            let mut items = Vec::with_capacity(total_columns);
            let last_column_index = row.cells.len().saturating_sub(1);

            for (column_index, cell) in row.cells.iter().enumerate() {
                let is_last_column = column_index == last_column_index;
                if is_editing_row {
                    if let Some(editor) = self.selected_row_editors.get(column_index).cloned() {
                        if is_last_column {
                            items.push(self.render_row_editor_cell_with_actions(
                                editor,
                                self.render_inline_update_actions(row_index, cx),
                                cx,
                            ));
                        } else {
                            items.push(render_database_input(editor, cx).into_any_element());
                        }
                    } else {
                        items.push(
                            Label::new(cell.display_text.clone())
                                .size(LabelSize::Small)
                                .into_any_element(),
                        );
                    }
                    continue;
                }

                let can_edit_inline = supports_update_delete;
                let display_text = cell.display_text.clone();
                let value_cell = div()
                    .id(format!(
                        "database-query-console-cell-{row_index}-{column_index}"
                    ))
                    .block_mouse_except_scroll()
                    .flex_1()
                    .min_w(px(0.))
                    .w_full()
                    .when(can_edit_inline, |this| {
                        this.cursor_pointer().on_click({
                            let query_console = query_console.clone();
                            move |event: &ClickEvent, window, cx| {
                                if event.click_count() > 1 {
                                    query_console.update(cx, |query_console, cx| {
                                        query_console.select_result_row(
                                            row_index,
                                            column_index,
                                            window,
                                            cx,
                                        );
                                    });
                                }
                            }
                        })
                    })
                    .child(Label::new(display_text).size(LabelSize::Small));

                if is_last_column && supports_update_delete {
                    items.push(
                        h_flex()
                            .w_full()
                            .min_w(px(0.))
                            .gap_1()
                            .items_center()
                            .child(value_cell)
                            .child(self.render_inline_delete_action(row_index, cx))
                            .into_any_element(),
                    );
                } else {
                    items.push(value_cell.into_any_element());
                }
            }
            rendered_rows.push(render_table_row(row_index + insert_row_offset, items));
        }

        let body = if rendered_rows.is_empty() {
            h_flex()
                .w_full()
                .p_3()
                .items_start()
                .justify_center()
                .child(
                    Label::new("This statement returned no rows.")
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                )
                .into_any_element()
        } else {
            v_flex().w_full().children(rendered_rows).into_any_element()
        };
        let table = div()
            .relative()
            .flex_none()
            .w(table_width)
            .min_w(table_width)
            .block_mouse_except_scroll()
            .map({
                let column_widths = column_widths.clone();
                move |this| bind_redistributable_columns(this, column_widths.clone())
            })
            .child(v_flex().w_full().child(header).child(body))
            .child(render_redistributable_columns_resize_handles(
                &column_widths,
                window,
                cx,
            ));

        div()
            .id("database-query-console-result-table-scroll")
            .flex_1()
            .w_full()
            .min_w(px(0.))
            .min_h(px(140.))
            .rounded_md()
            .border_1()
            .border_color(cx.theme().colors().border)
            .bg(cx.theme().colors().editor_background)
            .overflow_hidden()
            .track_scroll(&self.result_table_scroll_handle)
            .on_scroll_wheel({
                let scroll_handle = self.result_table_scroll_handle.clone();
                move |event, window, cx| {
                    let delta = event.delta.pixel_delta(window.line_height());
                    let current_offset = scroll_handle.offset();
                    let max_offset = scroll_handle.max_offset();
                    let next_offset = gpui::point(
                        (current_offset.x + delta.x).clamp(-max_offset.x, Pixels::ZERO),
                        (current_offset.y + delta.y).clamp(-max_offset.y, Pixels::ZERO),
                    );

                    if next_offset != current_offset {
                        scroll_handle.set_offset(next_offset);
                        cx.notify(query_console_entity_id);
                    }
                }
            })
            .child(table)
            .into_any_element()
    }
}

impl QueryConsoleResultTable {
    fn supports_insert(&self) -> bool {
        self.mutation_metadata
            .as_ref()
            .is_some_and(|mutation_metadata| {
                !self.column_names.is_empty()
                    && query_console_column_indices(
                        &mutation_metadata.table_column_names,
                        &self.column_names,
                    )
                    .is_some()
            })
    }

    fn supports_update_delete(&self) -> bool {
        let Some(mutation_metadata) = &self.mutation_metadata else {
            return false;
        };
        !mutation_metadata.key_column_names.is_empty()
            && query_console_column_indices(&self.column_names, &mutation_metadata.key_column_names)
                .is_some()
    }

    fn mutation_hint_message(&self) -> Option<String> {
        if let Some(mutation_message) = &self.mutation_message {
            return Some(mutation_message.clone());
        }
        if self.supports_update_delete() {
            Some(
                "Double-click a cell to edit inline, then click the check button to add SQL to the editor. Run the SQL above manually to persist it. Drag the Results divider or header dividers to resize."
                    .to_string(),
            )
        } else if self.supports_insert() {
            Some(
                "This result can draft INSERT statements. Use the check button to add INSERT SQL to the editor, then run it manually. Drag the Results divider or header dividers to resize."
                    .to_string(),
            )
        } else {
            None
        }
    }
}

impl EventEmitter<()> for DatabaseQueryConsole {}

impl Item for DatabaseQueryConsole {
    type Event = ();

    fn tab_content_text(&self, _detail: usize, _cx: &App) -> SharedString {
        format!("{} SQL", self.connection.name).into()
    }

    fn tab_tooltip_text(&self, _cx: &App) -> Option<SharedString> {
        Some(
            query_console_context_text(&self.connection, self.default_schema_name.as_deref())
                .into(),
        )
    }
}

impl Focusable for DatabaseQueryConsole {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.query_editor.focus_handle(cx)
    }
}

impl Render for DatabaseQueryConsole {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let query_console = cx.entity();
        let results_panel_height = clamp_query_console_results_panel_height(
            self.results_panel_height,
            self.effective_split_container_height(),
        );

        v_flex()
            .size_full()
            .bg(cx.theme().colors().editor_background)
            .p_2()
            .gap_2()
            .child(
                h_flex()
                    .w_full()
                    .justify_between()
                    .items_center()
                    .gap_3()
                    .child(
                        Label::new(query_console_context_text(
                            &self.connection,
                            self.default_schema_name.as_deref(),
                        ))
                        .size(LabelSize::Small)
                        .color(Color::Muted),
                    )
                    .child(
                        Label::new(self.status_message.clone())
                            .size(LabelSize::Small)
                            .color(notice_kind_color(self.status_kind)),
                    ),
            )
            .child(
                v_flex()
                    .id("database-query-console-split-container")
                    .relative()
                    .flex_1()
                    .min_h(px(0.))
                    .gap_1()
                    .on_drag_move::<DraggedQueryConsoleResultsResizeHandle>(cx.listener(
                        |this, e: &DragMoveEvent<DraggedQueryConsoleResultsResizeHandle>, _, cx| {
                            cx.stop_propagation();
                            this.resize_results_panel(
                                e.bounds.bottom() - e.event.position.y,
                                e.bounds.size.height,
                                cx,
                            );
                        },
                    ))
                    .child(
                        canvas(
                            move |bounds, _, cx| {
                                query_console.update(cx, |this, cx| {
                                    this.update_split_container_height(bounds.size.height, cx);
                                });
                            },
                            |_, _, _, _| {},
                        )
                        .absolute()
                        .size_full(),
                    )
                    .child(
                        v_flex()
                            .flex_1()
                            .min_h(QUERY_CONSOLE_MIN_SQL_PANEL_HEIGHT)
                            .gap_1()
                            .child(Label::new("SQL").size(LabelSize::Small).color(Color::Muted))
                            .child(render_database_fill_editor(self.query_editor.clone(), cx)),
                    )
                    .child(self.render_results_resize_handle(cx))
                    .child(
                        v_flex()
                            .h(results_panel_height)
                            .min_h(QUERY_CONSOLE_MIN_RESULTS_PANEL_HEIGHT)
                            .flex_shrink_0()
                            .gap_1()
                            .child(
                                Label::new("Results")
                                    .size(LabelSize::Small)
                                    .color(Color::Muted),
                            )
                            .child(
                                v_flex()
                                    .flex_1()
                                    .min_h(px(0.))
                                    .child(self.render_results_panel(window, cx)),
                            ),
                    ),
            )
    }
}

impl DatabaseQueryConsoleToolbarItemView {
    pub fn new() -> Self {
        Self {
            query_console: None,
            query_console_subscription: None,
        }
    }
}

impl EventEmitter<ToolbarItemEvent> for DatabaseQueryConsoleToolbarItemView {}

impl ToolbarItemView for DatabaseQueryConsoleToolbarItemView {
    fn set_active_pane_item(
        &mut self,
        active_pane_item: Option<&dyn ItemHandle>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> ToolbarItemLocation {
        if let Some(active_pane_item) = active_pane_item
            && let Some(query_console) = active_pane_item.downcast::<DatabaseQueryConsole>()
        {
            self.query_console = Some(query_console.clone());
            self.query_console_subscription = Some(cx.observe(&query_console, |_, _, cx| {
                cx.notify();
            }));
            return ToolbarItemLocation::PrimaryLeft;
        }

        self.query_console = None;
        self.query_console_subscription = None;
        cx.notify();
        ToolbarItemLocation::Hidden
    }
}

impl Render for DatabaseQueryConsoleToolbarItemView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let Some(query_console) = self.query_console.clone() else {
            return div();
        };

        let (is_running, has_results) = query_console.read_with(cx, |query_console, cx| {
            (
                matches!(query_console.status, QueryConsoleStatus::Running),
                query_console.output_editor.read(cx).text(cx) != QUERY_CONSOLE_EMPTY_RESULTS,
            )
        });

        h_flex()
            .items_center()
            .gap_2()
            .child(
                Button::new("database-query-console-run-selection", "Run Selection")
                    .size(ButtonSize::Compact)
                    .style(ButtonStyle::Filled)
                    .disabled(is_running)
                    .on_click(cx.listener({
                        let query_console = query_console.clone();
                        move |_, _, window, cx| {
                            query_console.update(cx, |query_console, cx| {
                                query_console.run_queries(
                                    QueryConsoleRunTarget::SelectionOrCurrent,
                                    window,
                                    cx,
                                );
                            });
                        }
                    })),
            )
            .child(
                Button::new("database-query-console-run-all", "Run All")
                    .size(ButtonSize::Compact)
                    .style(ButtonStyle::Filled)
                    .disabled(is_running)
                    .on_click(cx.listener({
                        let query_console = query_console.clone();
                        move |_, _, window, cx| {
                            query_console.update(cx, |query_console, cx| {
                                query_console.run_queries(QueryConsoleRunTarget::All, window, cx);
                            });
                        }
                    })),
            )
            .child(
                Button::new("database-query-console-clear-results", "Clear Results")
                    .size(ButtonSize::Compact)
                    .style(ButtonStyle::Subtle)
                    .disabled(is_running || !has_results)
                    .on_click(cx.listener({
                        let query_console = query_console.clone();
                        move |_, _, _, cx| {
                            query_console.update(cx, |query_console, cx| {
                                query_console.clear_results(cx);
                            });
                        }
                    })),
            )
    }
}

fn query_console_result_table_summary(result_table: &QueryConsoleResultTable) -> String {
    let row_message = match result_table.row_count {
        0 => "0 rows".to_string(),
        1 => "1 row".to_string(),
        count => format!("{count} rows"),
    };
    if result_table.truncated {
        format!(
            "{} · {row_message}, showing the first {QUERY_CONSOLE_MAX_ROWS} rows",
            statement_preview_text(&result_table.statement),
        )
    } else {
        format!(
            "{} · {row_message}",
            statement_preview_text(&result_table.statement)
        )
    }
}

fn query_console_result_table_shows_actions(result_table: &QueryConsoleResultTable) -> bool {
    let _ = result_table;
    false
}

fn query_console_result_table_data_column_widths(
    result_table: &QueryConsoleResultTable,
) -> Vec<Pixels> {
    let mut widths = result_table
        .column_names
        .iter()
        .enumerate()
        .map(|(column_index, column_name)| {
            let max_character_count = result_table
                .rows
                .iter()
                .filter_map(|row| row.cells.get(column_index))
                .map(|cell| cell.display_text.chars().count())
                .fold(column_name.chars().count(), usize::max);

            (px(max_character_count as f32 * QUERY_CONSOLE_RESULT_COLUMN_PIXELS_PER_CHARACTER)
                + QUERY_CONSOLE_RESULT_COLUMN_HORIZONTAL_PADDING)
                .clamp(
                    QUERY_CONSOLE_RESULT_COLUMN_MIN_WIDTH,
                    QUERY_CONSOLE_RESULT_COLUMN_MAX_WIDTH,
                )
        })
        .collect::<Vec<_>>();

    if (result_table.supports_update_delete() || result_table.supports_insert())
        && let Some(last_width) = widths.last_mut()
    {
        *last_width = (*last_width + QUERY_CONSOLE_INLINE_ACTIONS_EXTRA_WIDTH).clamp(
            QUERY_CONSOLE_LAST_COLUMN_MIN_WIDTH_WITH_INLINE_ACTIONS,
            QUERY_CONSOLE_LAST_COLUMN_MAX_WIDTH_WITH_INLINE_ACTIONS,
        );
    }

    widths
}

fn query_console_result_table_width(result_table: &QueryConsoleResultTable) -> Pixels {
    let mut width = query_console_result_table_data_column_widths(result_table)
        .into_iter()
        .fold(Pixels::ZERO, |width, column_width| width + column_width);

    if width == Pixels::ZERO {
        width = QUERY_CONSOLE_RESULT_COLUMN_MIN_WIDTH;
    }

    if query_console_result_table_shows_actions(result_table) {
        width += px(240.);
    }

    width
}

fn default_query_console_results_panel_height_value() -> f32 {
    f32::from(QUERY_CONSOLE_DEFAULT_RESULTS_PANEL_HEIGHT)
}

fn read_query_console_ui_state(cx: &App) -> SerializedQueryConsoleUiState {
    let kvp = KeyValueStore::global(cx);
    let scope = kvp.scoped(DATABASE_PANEL_KEY);
    let json = match scope.read(QUERY_CONSOLE_UI_STATE_KEY) {
        Ok(value) => value,
        Err(error) => {
            log::error!("Failed to read query console UI state: {error:#}");
            return SerializedQueryConsoleUiState::default();
        }
    };

    match json {
        Some(json) => match serde_json::from_str::<SerializedQueryConsoleUiState>(&json) {
            Ok(ui_state) => ui_state,
            Err(error) => {
                log::error!("Failed to deserialize query console UI state: {error:#}");
                SerializedQueryConsoleUiState::default()
            }
        },
        None => SerializedQueryConsoleUiState::default(),
    }
}

fn normalize_query_console_results_panel_height(results_panel_height: Pixels) -> Pixels {
    results_panel_height.max(QUERY_CONSOLE_MIN_RESULTS_PANEL_HEIGHT)
}

fn clamp_query_console_results_panel_height(
    results_panel_height: Pixels,
    available_height: Pixels,
) -> Pixels {
    let normalized_height = normalize_query_console_results_panel_height(results_panel_height);
    let max_height = (available_height
        - QUERY_CONSOLE_MIN_SQL_PANEL_HEIGHT
        - QUERY_CONSOLE_RESULTS_RESIZE_HANDLE_SIZE)
        .max(QUERY_CONSOLE_MIN_RESULTS_PANEL_HEIGHT);

    normalized_height.min(max_height)
}

fn create_query_console_result_table_column_widths<T>(
    result_table: &QueryConsoleResultTable,
    cx: &mut Context<T>,
) -> Entity<RedistributableColumnsState> {
    let shows_actions = query_console_result_table_shows_actions(result_table);
    let mut widths = query_console_result_table_data_column_widths(result_table);
    let mut resize_behavior = vec![TableResizeBehavior::Resizable; widths.len()];
    if (result_table.supports_update_delete() || result_table.supports_insert())
        && let Some(last_resize_behavior) = resize_behavior.last_mut()
    {
        *last_resize_behavior =
            TableResizeBehavior::MinSize(QUERY_CONSOLE_LAST_COLUMN_MIN_FRACTION);
    }
    if shows_actions {
        widths.push(px(240.));
        resize_behavior.push(TableResizeBehavior::MinSize(0.16));
    }

    cx.new(|_| RedistributableColumnsState::new(widths.len(), widths, resize_behavior))
}

fn create_query_console_row_editors<T>(
    connection_kind: DatabaseKind,
    column_names: &[String],
    row: Option<&QueryConsoleResultRow>,
    window: &mut Window,
    cx: &mut Context<T>,
) -> Vec<Entity<Editor>> {
    column_names
        .iter()
        .enumerate()
        .map(|(column_index, column_name)| {
            let initial_value = row
                .and_then(|row| row.cells.get(column_index))
                .and_then(|cell| query_console_editable_cell_text(connection_kind, cell));
            let placeholder_text = if row
                .and_then(|row| row.cells.get(column_index))
                .is_some_and(|cell| matches!(cell.sql_value, QueryConsoleSqlValue::Null))
            {
                "NULL".to_string()
            } else {
                column_name.clone()
            };

            cx.new(|cx| {
                let mut editor = Editor::single_line(window, cx);
                editor.set_placeholder_text(&placeholder_text, window, cx);
                editor.set_show_edit_predictions(Some(false), window, cx);
                if let Some(initial_value) = initial_value.clone() {
                    editor.set_text(initial_value, window, cx);
                }
                editor
            })
        })
        .collect()
}

fn query_console_editable_cell_text(
    connection_kind: DatabaseKind,
    cell: &QueryConsoleCellValue,
) -> Option<String> {
    query_console_sql_value_editable_text(connection_kind, &cell.sql_value)
}

fn read_query_console_row_editor_values(editors: &[Entity<Editor>], cx: &App) -> Vec<String> {
    editors
        .iter()
        .map(|editor| editor.read(cx).text(cx))
        .collect()
}

fn append_query_console_statement_text(existing_text: &str, statement_sql: &str) -> String {
    let trimmed_existing_text = existing_text.trim();
    if trimmed_existing_text.is_empty() {
        statement_sql.to_string()
    } else {
        format!("{trimmed_existing_text}\n\n{}", statement_sql.trim())
    }
}

fn build_query_console_update_statement(
    connection_kind: DatabaseKind,
    result_table: &QueryConsoleResultTable,
    original_row: &QueryConsoleResultRow,
    edited_values: &[String],
) -> Result<String> {
    let mutation_metadata = result_table
        .mutation_metadata
        .as_ref()
        .context("This result is read-only.")?;
    let where_clause =
        build_query_console_where_clause(connection_kind, result_table, original_row)?;

    let mut assignments = Vec::new();
    for (column_index, column_name) in result_table.column_names.iter().enumerate() {
        let Some(original_cell) = original_row.cells.get(column_index) else {
            continue;
        };
        let edited_value = edited_values.get(column_index).cloned().unwrap_or_default();
        if query_console_editor_value_matches_cell(connection_kind, &edited_value, original_cell) {
            continue;
        }

        assignments.push(format!(
            "{} = {}",
            query_console_quote_column_name(connection_kind, column_name),
            query_console_input_to_sql_literal(
                connection_kind,
                &edited_value,
                Some(&original_cell.sql_value)
                    .filter(|sql_value| !matches!(sql_value, QueryConsoleSqlValue::Null))
                    .or_else(|| query_console_column_value_hint(result_table, column_index)),
            ),
        ));
    }

    if assignments.is_empty() {
        return Err(anyhow!("There are no row changes to append."));
    }

    Ok(format!(
        "UPDATE {} SET {} WHERE {};",
        query_console_quote_table_path(connection_kind, &mutation_metadata.table_path),
        assignments.join(", "),
        where_clause
    ))
}

fn build_query_console_delete_statement(
    connection_kind: DatabaseKind,
    result_table: &QueryConsoleResultTable,
    original_row: &QueryConsoleResultRow,
) -> Result<String> {
    let mutation_metadata = result_table
        .mutation_metadata
        .as_ref()
        .context("This result is read-only.")?;
    let where_clause =
        build_query_console_where_clause(connection_kind, result_table, original_row)?;

    Ok(format!(
        "DELETE FROM {} WHERE {};",
        query_console_quote_table_path(connection_kind, &mutation_metadata.table_path),
        where_clause
    ))
}

fn build_query_console_insert_statement(
    connection_kind: DatabaseKind,
    result_table: &QueryConsoleResultTable,
    edited_values: &[String],
) -> Result<String> {
    let mutation_metadata = result_table
        .mutation_metadata
        .as_ref()
        .context("This result is read-only.")?;

    let mut columns = Vec::new();
    let mut values = Vec::new();
    for (column_index, column_name) in result_table.column_names.iter().enumerate() {
        let edited_value = edited_values.get(column_index).cloned().unwrap_or_default();
        if edited_value.trim().is_empty() {
            continue;
        }

        columns.push(query_console_quote_column_name(
            connection_kind,
            column_name,
        ));
        values.push(query_console_input_to_sql_literal(
            connection_kind,
            &edited_value,
            query_console_column_value_hint(result_table, column_index),
        ));
    }

    if columns.is_empty() {
        return Err(anyhow!("Enter at least one value before appending INSERT."));
    }

    Ok(format!(
        "INSERT INTO {} ({}) VALUES ({});",
        query_console_quote_table_path(connection_kind, &mutation_metadata.table_path),
        columns.join(", "),
        values.join(", ")
    ))
}

fn build_query_console_where_clause(
    connection_kind: DatabaseKind,
    result_table: &QueryConsoleResultTable,
    original_row: &QueryConsoleResultRow,
) -> Result<String> {
    let mutation_metadata = result_table
        .mutation_metadata
        .as_ref()
        .context("This result is read-only.")?;
    let key_column_indices = query_console_column_indices(
        &result_table.column_names,
        &mutation_metadata.key_column_names,
    )
    .with_context(|| {
        format!(
            "UPDATE and DELETE require the key columns in the result: {}.",
            mutation_metadata.key_column_names.join(", ")
        )
    })?;

    let mut conditions = Vec::with_capacity(key_column_indices.len());
    for column_index in key_column_indices {
        let column_name = &result_table.column_names[column_index];
        let cell = original_row
            .cells
            .get(column_index)
            .with_context(|| format!("Could not read the key column {column_name}."))?;
        conditions.push(format!(
            "{} = {}",
            query_console_quote_column_name(connection_kind, column_name),
            cell.sql_value.to_sql_literal(connection_kind),
        ));
    }

    if conditions.is_empty() {
        Err(anyhow!(
            "This result does not include a usable key for row mutations."
        ))
    } else {
        Ok(conditions.join(" AND "))
    }
}

fn query_console_editor_value_matches_cell(
    connection_kind: DatabaseKind,
    edited_value: &str,
    original_cell: &QueryConsoleCellValue,
) -> bool {
    match &original_cell.sql_value {
        QueryConsoleSqlValue::Null => edited_value.trim().is_empty(),
        _ => query_console_sql_value_editable_text(connection_kind, &original_cell.sql_value)
            .is_some_and(|original_value| edited_value == original_value),
    }
}

fn query_console_sql_value_editable_text(
    connection_kind: DatabaseKind,
    sql_value: &QueryConsoleSqlValue,
) -> Option<String> {
    match sql_value {
        QueryConsoleSqlValue::Null => None,
        QueryConsoleSqlValue::Number(value) | QueryConsoleSqlValue::Text(value) => {
            Some(value.clone())
        }
        QueryConsoleSqlValue::Bytes(bytes) => Some(match connection_kind {
            DatabaseKind::MySql => format!("0x{}", encode_query_console_hex(bytes)),
            DatabaseKind::Postgres => format!("\\x{}", encode_query_console_hex(bytes)),
        }),
    }
}

fn query_console_column_indices(
    column_names: &[String],
    target_column_names: &[String],
) -> Option<Vec<usize>> {
    let mut indices = Vec::with_capacity(target_column_names.len());
    for target_column_name in target_column_names {
        indices.push(query_console_column_index(
            column_names,
            target_column_name,
        )?);
    }
    Some(indices)
}

fn query_console_column_index(column_names: &[String], target_column_name: &str) -> Option<usize> {
    column_names.iter().position(|column_name| {
        column_name == target_column_name || column_name.eq_ignore_ascii_case(target_column_name)
    })
}

fn query_console_column_value_hint(
    result_table: &QueryConsoleResultTable,
    column_index: usize,
) -> Option<&QueryConsoleSqlValue> {
    result_table
        .rows
        .iter()
        .filter_map(|row| row.cells.get(column_index))
        .map(|cell| &cell.sql_value)
        .find(|sql_value| !matches!(sql_value, QueryConsoleSqlValue::Null))
}

fn query_console_input_to_sql_literal(
    connection_kind: DatabaseKind,
    edited_value: &str,
    original_value: Option<&QueryConsoleSqlValue>,
) -> String {
    let trimmed_value = edited_value.trim();
    if trimmed_value.is_empty() {
        return "NULL".to_string();
    }

    match original_value {
        Some(QueryConsoleSqlValue::Number(_))
            if looks_like_query_console_numeric_literal(trimmed_value) =>
        {
            trimmed_value.to_string()
        }
        Some(QueryConsoleSqlValue::Bytes(_)) => {
            query_console_bytes_input_to_sql_literal(connection_kind, edited_value)
        }
        _ => query_console_quote_string_literal(connection_kind, edited_value),
    }
}

fn query_console_input_to_sql_value(
    connection_kind: DatabaseKind,
    edited_value: &str,
    original_value: Option<&QueryConsoleSqlValue>,
) -> QueryConsoleSqlValue {
    let trimmed_value = edited_value.trim();
    if trimmed_value.is_empty() {
        return QueryConsoleSqlValue::Null;
    }

    match original_value {
        Some(QueryConsoleSqlValue::Number(_))
            if looks_like_query_console_numeric_literal(trimmed_value) =>
        {
            QueryConsoleSqlValue::Number(trimmed_value.to_string())
        }
        Some(QueryConsoleSqlValue::Bytes(_)) => {
            query_console_bytes_input_to_sql_value(connection_kind, edited_value)
                .unwrap_or_else(|| QueryConsoleSqlValue::Text(edited_value.to_string()))
        }
        _ => QueryConsoleSqlValue::Text(edited_value.to_string()),
    }
}

fn query_console_bytes_input_to_sql_value(
    connection_kind: DatabaseKind,
    edited_value: &str,
) -> Option<QueryConsoleSqlValue> {
    let trimmed_value = edited_value.trim();
    match connection_kind {
        DatabaseKind::MySql
            if trimmed_value.starts_with("0x") || trimmed_value.starts_with("0X") =>
        {
            decode_query_console_hex_literal(&trimmed_value[2..]).map(QueryConsoleSqlValue::Bytes)
        }
        DatabaseKind::Postgres if trimmed_value.starts_with("\\x") => {
            decode_query_console_hex_literal(&trimmed_value[2..]).map(QueryConsoleSqlValue::Bytes)
        }
        _ => None,
    }
}

fn decode_query_console_hex_literal(hex: &str) -> Option<Vec<u8>> {
    let hex = hex.trim();
    if hex.is_empty() || hex.len() % 2 != 0 {
        return None;
    }

    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let mut index = 0;
    while index < hex.len() {
        let byte = u8::from_str_radix(&hex[index..index + 2], 16).ok()?;
        bytes.push(byte);
        index += 2;
    }
    Some(bytes)
}

fn query_console_build_draft_row(
    connection_kind: DatabaseKind,
    result_table: &QueryConsoleResultTable,
    edited_values: &[String],
    original_row: Option<&QueryConsoleResultRow>,
) -> QueryConsoleResultRow {
    let cells = result_table
        .column_names
        .iter()
        .enumerate()
        .map(|(column_index, _)| {
            let edited_value = edited_values.get(column_index).cloned().unwrap_or_default();
            let original_value = original_row
                .and_then(|row| row.cells.get(column_index))
                .map(|cell| &cell.sql_value)
                .filter(|sql_value| !matches!(sql_value, QueryConsoleSqlValue::Null))
                .or_else(|| query_console_column_value_hint(result_table, column_index));
            query_console_cell_value(query_console_input_to_sql_value(
                connection_kind,
                &edited_value,
                original_value,
            ))
        })
        .collect();

    QueryConsoleResultRow { cells }
}

fn query_console_apply_local_row_update(
    connection_kind: DatabaseKind,
    result_table: &mut QueryConsoleResultTable,
    row_index: usize,
    edited_values: &[String],
    original_row: &QueryConsoleResultRow,
) {
    let updated_row = query_console_build_draft_row(
        connection_kind,
        result_table,
        edited_values,
        Some(original_row),
    );
    if let Some(row) = result_table.rows.get_mut(row_index) {
        *row = updated_row;
    }
}

fn query_console_apply_local_row_insert(
    connection_kind: DatabaseKind,
    result_table: &mut QueryConsoleResultTable,
    edited_values: &[String],
) {
    let inserted_row =
        query_console_build_draft_row(connection_kind, result_table, edited_values, None);
    result_table.rows.push(inserted_row);
    result_table.row_count += 1;
}

fn query_console_apply_local_row_delete(
    result_table: &mut QueryConsoleResultTable,
    row_index: usize,
) {
    if row_index < result_table.rows.len() {
        result_table.rows.remove(row_index);
        result_table.row_count = result_table.row_count.saturating_sub(1);
    }
}

fn looks_like_query_console_numeric_literal(value: &str) -> bool {
    value.parse::<i64>().is_ok() || value.parse::<u64>().is_ok() || value.parse::<f64>().is_ok()
}

fn query_console_bytes_input_to_sql_literal(
    connection_kind: DatabaseKind,
    edited_value: &str,
) -> String {
    let trimmed_value = edited_value.trim();
    match connection_kind {
        DatabaseKind::MySql
            if trimmed_value.starts_with("0x") || trimmed_value.starts_with("0X") =>
        {
            trimmed_value.to_string()
        }
        DatabaseKind::Postgres if trimmed_value.starts_with("\\x") => {
            format!("'{}'::bytea", trimmed_value)
        }
        _ => query_console_quote_string_literal(connection_kind, edited_value),
    }
}

fn query_console_quote_table_path(
    connection_kind: DatabaseKind,
    table_path: &DatabaseTablePath,
) -> String {
    match connection_kind {
        DatabaseKind::MySql => {
            format_mysql_qualified_name(&table_path.schema_name, &table_path.table_name)
        }
        DatabaseKind::Postgres => format!(
            "{}.{}",
            quote_postgres_identifier(&table_path.schema_name),
            quote_postgres_identifier(&table_path.table_name)
        ),
    }
}

fn query_console_quote_column_name(connection_kind: DatabaseKind, column_name: &str) -> String {
    match connection_kind {
        DatabaseKind::MySql => quote_mysql_identifier(column_name),
        DatabaseKind::Postgres => quote_postgres_identifier(column_name),
    }
}

fn query_console_quote_string_literal(connection_kind: DatabaseKind, value: &str) -> String {
    match connection_kind {
        DatabaseKind::MySql => quote_mysql_string_literal(value),
        DatabaseKind::Postgres => quote_postgres_string_literal(value),
    }
}

fn query_console_mutation_metadata_from_browser_details(
    table_path: &DatabaseTablePath,
    browser_details: &DatabaseTableBrowserDetails,
    result_column_names: &[String],
) -> (Option<QueryConsoleTableMutationMetadata>, Option<String>) {
    let table_column_names = browser_details
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if result_column_names.iter().any(|result_column_name| {
        !table_column_names.iter().any(|table_column_name| {
            table_column_name == result_column_name
                || table_column_name.eq_ignore_ascii_case(result_column_name)
        })
    }) {
        return (
            None,
            Some(
                "Row mutations are available only when the result columns map directly to table columns."
                    .to_string(),
            ),
        );
    }

    let key_column_names = preferred_query_console_key_columns(&browser_details.keys);
    (
        Some(QueryConsoleTableMutationMetadata {
            table_path: table_path.clone(),
            table_column_names,
            key_column_names,
        }),
        None,
    )
}

fn preferred_query_console_key_columns(keys: &[DatabaseTableKey]) -> Vec<String> {
    keys.iter()
        .find(|key| key.constraint_type.eq_ignore_ascii_case("PRIMARY KEY"))
        .or_else(|| {
            keys.iter()
                .find(|key| key.constraint_type.eq_ignore_ascii_case("UNIQUE"))
        })
        .map(|key| key.column_names.clone())
        .unwrap_or_default()
}

impl QueryConsoleSqlValue {
    fn to_sql_literal(&self, connection_kind: DatabaseKind) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Number(value) => value.clone(),
            Self::Text(value) => query_console_quote_string_literal(connection_kind, value),
            Self::Bytes(bytes) => match connection_kind {
                DatabaseKind::MySql => format!("0x{}", encode_query_console_hex(bytes)),
                DatabaseKind::Postgres => {
                    format!("'\\\\x{}'::bytea", encode_query_console_hex(bytes))
                }
            },
        }
    }
}

fn encode_query_console_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02X}"));
    }
    output
}

fn quote_postgres_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

async fn execute_query_console_statements(
    connection_kind: DatabaseKind,
    connection_url: String,
    default_schema_name: Option<String>,
    target_table_path: Option<DatabaseTablePath>,
    statement_sqls: Vec<String>,
) -> Result<QueryConsoleExecutionSummary> {
    match connection_kind {
        DatabaseKind::MySql => {
            execute_mysql_query_console_statements(
                connection_url,
                default_schema_name,
                target_table_path,
                statement_sqls,
            )
            .await
        }
        DatabaseKind::Postgres => {
            execute_postgres_query_console_statements(
                connection_url,
                default_schema_name,
                target_table_path,
                statement_sqls,
            )
            .await
        }
    }
}

async fn build_query_console_result_table(
    connection_kind: DatabaseKind,
    connection_url: &str,
    target_table_path: Option<&DatabaseTablePath>,
    execution: &QueryStatementExecution,
) -> Option<QueryConsoleResultTable> {
    if execution.column_names.is_empty() {
        return None;
    }

    let (mutation_metadata, mutation_message) = if let Some(table_path) = target_table_path {
        match load_table_browser_details(
            connection_kind,
            connection_url.to_string(),
            table_path.schema_name.clone(),
            table_path.table_name.clone(),
        )
        .await
        {
            Ok(browser_details) => query_console_mutation_metadata_from_browser_details(
                table_path,
                &browser_details,
                &execution.column_names,
            ),
            Err(error) => (
                None,
                Some(format!(
                    "Could not load metadata for {}.{}: {}. Row mutations are read-only.",
                    table_path.schema_name,
                    table_path.table_name,
                    format_connection_error(&error, connection_url)
                )),
            ),
        }
    } else {
        (None, None)
    };

    Some(QueryConsoleResultTable {
        statement: execution.statement.clone(),
        column_names: execution.column_names.clone(),
        rows: execution.rows.clone(),
        row_count: execution.row_count,
        truncated: execution.truncated,
        mutation_metadata,
        mutation_message,
    })
}

async fn execute_mysql_query_console_statements(
    connection_url: String,
    default_schema_name: Option<String>,
    target_table_path: Option<DatabaseTablePath>,
    statement_sqls: Vec<String>,
) -> Result<QueryConsoleExecutionSummary> {
    let mut connection = MySqlConnection::connect(&connection_url)
        .await
        .context("Could not connect to MySQL")?;
    if let Some(default_schema_name) = default_schema_name.as_deref() {
        let statement = format!("USE {}", quote_mysql_identifier(default_schema_name));
        raw_sql(&statement)
            .execute(&mut connection)
            .await
            .with_context(|| format!("Could not select MySQL database {default_schema_name}"))?;
    }

    let total_statement_count = statement_sqls.len();
    let mut sections = Vec::with_capacity(total_statement_count);
    let mut result_table = None;
    for (index, statement_sql) in statement_sqls.into_iter().enumerate() {
        match execute_mysql_query_console_statement(&mut connection, &statement_sql).await {
            Ok(statement_execution) => {
                result_table = build_query_console_result_table(
                    DatabaseKind::MySql,
                    &connection_url,
                    target_table_path.as_ref(),
                    &statement_execution,
                )
                .await
                .or(result_table);
                sections.push(format_query_console_statement_output(
                    index + 1,
                    &statement_execution,
                ));
            }
            Err(error) => {
                let error_message = error.to_string();
                sections.push(format_query_console_statement_error(
                    index + 1,
                    &statement_sql,
                    &error_message,
                ));
                return Ok(QueryConsoleExecutionSummary {
                    output_text: sections.join("\n\n"),
                    completed_statement_count: index,
                    stopped_at_statement_number: Some(index + 1),
                    error_message: Some(error_message),
                    result_table,
                });
            }
        }
    }

    Ok(QueryConsoleExecutionSummary {
        output_text: sections.join("\n\n"),
        completed_statement_count: total_statement_count,
        stopped_at_statement_number: None,
        error_message: None,
        result_table,
    })
}

async fn execute_postgres_query_console_statements(
    connection_url: String,
    default_schema_name: Option<String>,
    target_table_path: Option<DatabaseTablePath>,
    statement_sqls: Vec<String>,
) -> Result<QueryConsoleExecutionSummary> {
    let mut connection = PgConnection::connect(&connection_url)
        .await
        .context("Could not connect to Postgres")?;
    if let Some(default_schema_name) = default_schema_name.as_deref() {
        let statement = format!(
            "SET search_path TO {}",
            quote_postgres_identifier(default_schema_name)
        );
        raw_sql(&statement)
            .execute(&mut connection)
            .await
            .with_context(|| {
                format!("Could not set Postgres search_path to schema {default_schema_name}")
            })?;
    }

    let total_statement_count = statement_sqls.len();
    let mut sections = Vec::with_capacity(total_statement_count);
    let mut result_table = None;
    for (index, statement_sql) in statement_sqls.into_iter().enumerate() {
        match execute_postgres_query_console_statement(&mut connection, &statement_sql).await {
            Ok(statement_execution) => {
                result_table = build_query_console_result_table(
                    DatabaseKind::Postgres,
                    &connection_url,
                    target_table_path.as_ref(),
                    &statement_execution,
                )
                .await
                .or(result_table);
                sections.push(format_query_console_statement_output(
                    index + 1,
                    &statement_execution,
                ));
            }
            Err(error) => {
                let error_message = error.to_string();
                sections.push(format_query_console_statement_error(
                    index + 1,
                    &statement_sql,
                    &error_message,
                ));
                return Ok(QueryConsoleExecutionSummary {
                    output_text: sections.join("\n\n"),
                    completed_statement_count: index,
                    stopped_at_statement_number: Some(index + 1),
                    error_message: Some(error_message),
                    result_table,
                });
            }
        }
    }

    Ok(QueryConsoleExecutionSummary {
        output_text: sections.join("\n\n"),
        completed_statement_count: total_statement_count,
        stopped_at_statement_number: None,
        error_message: None,
        result_table,
    })
}

async fn execute_mysql_query_console_statement(
    connection: &mut MySqlConnection,
    statement: &str,
) -> Result<QueryStatementExecution> {
    let mut column_names = Vec::new();
    let mut rows = Vec::new();
    let mut rows_affected = 0;
    let mut row_count = 0;
    let mut truncated = false;
    let mut stream = raw_sql(statement).fetch_many(&mut *connection);

    while let Some(step) = stream.try_next().await? {
        match step {
            Either::Left(query_result) => {
                rows_affected = query_result.rows_affected();
            }
            Either::Right(row) => {
                if column_names.is_empty() {
                    column_names = row
                        .columns()
                        .iter()
                        .map(|column| truncate_query_console_cell(column.name()))
                        .collect();
                }

                row_count += 1;
                if rows.len() < QUERY_CONSOLE_MAX_ROWS {
                    rows.push(mysql_query_console_row_values(&row));
                } else {
                    truncated = true;
                }
            }
        }
    }
    drop(stream);

    if column_names.is_empty() && statement_likely_returns_rows(statement) {
        if let Ok(description) = connection.describe(statement).await {
            column_names = description
                .columns()
                .iter()
                .map(|column| truncate_query_console_cell(column.name()))
                .collect();
        }
    }

    Ok(QueryStatementExecution {
        statement: statement.to_string(),
        column_names,
        rows,
        rows_affected,
        row_count,
        truncated,
    })
}

async fn execute_postgres_query_console_statement(
    connection: &mut PgConnection,
    statement: &str,
) -> Result<QueryStatementExecution> {
    let mut column_names = Vec::new();
    let mut rows = Vec::new();
    let mut rows_affected = 0;
    let mut row_count = 0;
    let mut truncated = false;
    let mut stream = raw_sql(statement).fetch_many(&mut *connection);

    while let Some(step) = stream.try_next().await? {
        match step {
            Either::Left(query_result) => {
                rows_affected = query_result.rows_affected();
            }
            Either::Right(row) => {
                if column_names.is_empty() {
                    column_names = row
                        .columns()
                        .iter()
                        .map(|column| truncate_query_console_cell(column.name()))
                        .collect();
                }

                row_count += 1;
                if rows.len() < QUERY_CONSOLE_MAX_ROWS {
                    rows.push(postgres_query_console_row_values(&row));
                } else {
                    truncated = true;
                }
            }
        }
    }
    drop(stream);

    if column_names.is_empty() && statement_likely_returns_rows(statement) {
        if let Ok(description) = connection.describe(statement).await {
            column_names = description
                .columns()
                .iter()
                .map(|column| truncate_query_console_cell(column.name()))
                .collect();
        }
    }

    Ok(QueryStatementExecution {
        statement: statement.to_string(),
        column_names,
        rows,
        rows_affected,
        row_count,
        truncated,
    })
}

fn mysql_query_console_row_values(row: &MySqlRow) -> QueryConsoleResultRow {
    let mut cells = Vec::with_capacity(row.columns().len());
    for index in 0..row.columns().len() {
        cells.push(format_mysql_query_console_row_value(row, index));
    }
    QueryConsoleResultRow { cells }
}

fn postgres_query_console_row_values(row: &PgRow) -> QueryConsoleResultRow {
    let mut cells = Vec::with_capacity(row.columns().len());
    for index in 0..row.columns().len() {
        cells.push(format_postgres_query_console_row_value(row, index));
    }
    QueryConsoleResultRow { cells }
}

fn format_mysql_query_console_row_value(row: &MySqlRow, index: usize) -> QueryConsoleCellValue {
    let Ok(raw_value) = row.try_get_raw(index) else {
        return query_console_cell_value(QueryConsoleSqlValue::Text("<unavailable>".to_string()));
    };
    if raw_value.is_null() {
        return query_console_cell_value(QueryConsoleSqlValue::Null);
    }

    let type_name = raw_value.type_info().name().to_ascii_uppercase();
    let sql_value = if matches!(
        type_name.as_str(),
        "TINYINT UNSIGNED"
            | "SMALLINT UNSIGNED"
            | "INT UNSIGNED"
            | "MEDIUMINT UNSIGNED"
            | "BIGINT UNSIGNED"
    ) {
        row.try_get::<u64, _>(index)
            .map(|value| QueryConsoleSqlValue::Number(value.to_string()))
            .unwrap_or_else(|_| mysql_query_console_text_sql_value(row, index, &type_name))
    } else if matches!(
        type_name.as_str(),
        "BOOLEAN" | "TINYINT" | "SMALLINT" | "INT" | "MEDIUMINT" | "BIGINT" | "YEAR"
    ) {
        row.try_get::<i64, _>(index)
            .map(|value| QueryConsoleSqlValue::Number(value.to_string()))
            .unwrap_or_else(|_| mysql_query_console_text_sql_value(row, index, &type_name))
    } else if matches!(type_name.as_str(), "FLOAT" | "DOUBLE") {
        row.try_get::<f64, _>(index)
            .map(|value| QueryConsoleSqlValue::Number(value.to_string()))
            .unwrap_or_else(|_| mysql_query_console_text_sql_value(row, index, &type_name))
    } else if matches!(
        type_name.as_str(),
        "TIMESTAMP" | "DATETIME" | "DATE" | "TIME"
    ) {
        mysql_query_console_temporal_sql_value(row, index, &type_name)
            .unwrap_or_else(|| mysql_query_console_text_sql_value(row, index, &type_name))
    } else if matches!(
        type_name.as_str(),
        "BIT"
            | "BINARY"
            | "VARBINARY"
            | "BLOB"
            | "TINYBLOB"
            | "MEDIUMBLOB"
            | "LONGBLOB"
            | "GEOMETRY"
    ) {
        row.try_get::<Vec<u8>, _>(index)
            .map(QueryConsoleSqlValue::Bytes)
            .unwrap_or_else(|_| QueryConsoleSqlValue::Text("<binary>".to_string()))
    } else {
        mysql_query_console_text_sql_value(row, index, &type_name)
    };

    query_console_cell_value(sql_value)
}

fn mysql_query_console_text_sql_value(
    row: &MySqlRow,
    index: usize,
    type_name: &str,
) -> QueryConsoleSqlValue {
    let value = mysql_query_console_text_value(row, index);
    if matches!(type_name, "DECIMAL" | "NUMERIC")
        && looks_like_query_console_numeric_literal(value.trim())
    {
        QueryConsoleSqlValue::Number(value)
    } else {
        QueryConsoleSqlValue::Text(value)
    }
}

fn mysql_query_console_text_value(row: &MySqlRow, index: usize) -> String {
    if let Ok(value) = row.try_get::<String, _>(index) {
        return value;
    }

    if let Ok(bytes) = row.try_get::<Vec<u8>, _>(index) {
        return format_query_console_text_bytes(&bytes);
    }

    "<value>".to_string()
}

fn mysql_query_console_temporal_sql_value(
    row: &MySqlRow,
    index: usize,
    type_name: &str,
) -> Option<QueryConsoleSqlValue> {
    let value = match type_name {
        "TIMESTAMP" => row
            .try_get::<DateTime<Utc>, _>(index)
            .ok()
            .map(format_query_console_utc_datetime),
        "DATETIME" => row
            .try_get::<NaiveDateTime, _>(index)
            .ok()
            .map(|value| value.to_string()),
        "DATE" => row
            .try_get::<NaiveDate, _>(index)
            .ok()
            .map(|value| value.to_string()),
        "TIME" => row
            .try_get::<NaiveTime, _>(index)
            .ok()
            .map(|value| value.to_string()),
        _ => None,
    }?;

    Some(QueryConsoleSqlValue::Text(value))
}

fn format_postgres_query_console_row_value(row: &PgRow, index: usize) -> QueryConsoleCellValue {
    let Ok(raw_value) = row.try_get_raw(index) else {
        return query_console_cell_value(QueryConsoleSqlValue::Text("<unavailable>".to_string()));
    };
    if raw_value.is_null() {
        return query_console_cell_value(QueryConsoleSqlValue::Null);
    }

    let type_name = raw_value.type_info().name().to_ascii_uppercase();
    let sql_value = if type_name.eq_ignore_ascii_case("BYTEA") {
        row.try_get::<Vec<u8>, _>(index)
            .map(QueryConsoleSqlValue::Bytes)
            .unwrap_or_else(|_| QueryConsoleSqlValue::Text("<bytea>".to_string()))
    } else if matches!(
        type_name.as_str(),
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATE" | "TIME"
    ) {
        postgres_query_console_temporal_sql_value(row, index, &type_name)
            .unwrap_or_else(|| QueryConsoleSqlValue::Text("<value>".to_string()))
    } else {
        let value = raw_value
            .as_bytes()
            .map(format_query_console_text_bytes)
            .unwrap_or_else(|_| "<value>".to_string());
        if matches!(
            type_name.as_str(),
            "INT2"
                | "INT4"
                | "INT8"
                | "INTEGER"
                | "BIGINT"
                | "SMALLINT"
                | "OID"
                | "FLOAT4"
                | "FLOAT8"
                | "REAL"
                | "DOUBLE PRECISION"
                | "NUMERIC"
                | "DECIMAL"
        ) && looks_like_query_console_numeric_literal(value.trim())
        {
            QueryConsoleSqlValue::Number(value)
        } else {
            QueryConsoleSqlValue::Text(value)
        }
    };

    query_console_cell_value(sql_value)
}

fn postgres_query_console_temporal_sql_value(
    row: &PgRow,
    index: usize,
    type_name: &str,
) -> Option<QueryConsoleSqlValue> {
    let value = match type_name {
        "TIMESTAMPTZ" => row
            .try_get::<DateTime<Utc>, _>(index)
            .ok()
            .map(format_query_console_utc_datetime),
        "TIMESTAMP" => row
            .try_get::<NaiveDateTime, _>(index)
            .ok()
            .map(|value| value.to_string()),
        "DATE" => row
            .try_get::<NaiveDate, _>(index)
            .ok()
            .map(|value| value.to_string()),
        "TIME" => row
            .try_get::<NaiveTime, _>(index)
            .ok()
            .map(|value| value.to_string()),
        _ => None,
    }?;

    Some(QueryConsoleSqlValue::Text(value))
}

fn format_query_console_utc_datetime(value: DateTime<Utc>) -> String {
    value.naive_utc().to_string()
}

fn query_console_cell_value(sql_value: QueryConsoleSqlValue) -> QueryConsoleCellValue {
    let display_text = query_console_display_text(&sql_value);
    QueryConsoleCellValue {
        display_text,
        sql_value,
    }
}

fn query_console_display_text(sql_value: &QueryConsoleSqlValue) -> String {
    let raw_display = match sql_value {
        QueryConsoleSqlValue::Null => "NULL".to_string(),
        QueryConsoleSqlValue::Number(value) | QueryConsoleSqlValue::Text(value) => value.clone(),
        QueryConsoleSqlValue::Bytes(bytes) => format_query_console_blob(bytes),
    };
    truncate_query_console_cell(&sanitize_query_console_cell(&raw_display))
}

fn format_query_console_text_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => String::from_utf8_lossy(bytes).into_owned(),
    }
}

fn format_query_console_blob(value: &[u8]) -> String {
    let mut formatted = String::from("0x");
    let mut displayed_byte_count = 0;
    for byte in value.iter().take(16) {
        formatted.push_str(&format!("{byte:02X}"));
        displayed_byte_count += 1;
    }
    if value.len() > displayed_byte_count {
        formatted.push_str("...");
        formatted.push_str(&format!(" ({} bytes)", value.len()));
    }
    formatted
}

fn sanitize_query_console_cell(value: &str) -> String {
    value
        .replace('\r', "\\r")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('|', "¦")
}

fn truncate_query_console_cell(value: &str) -> String {
    if value.chars().count() <= QUERY_CONSOLE_MAX_CELL_WIDTH {
        return value.to_string();
    }

    let truncated: String = value
        .chars()
        .take(QUERY_CONSOLE_MAX_CELL_WIDTH.saturating_sub(3))
        .collect();
    format!("{truncated}...")
}

fn format_query_console_statement_output(
    statement_number: usize,
    execution: &QueryStatementExecution,
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Statement {statement_number}"));
    lines.push(statement_preview_text(&execution.statement));

    if !execution.column_names.is_empty() {
        lines.push(String::new());
        lines.push(format_query_console_result_table(
            &execution.column_names,
            &execution.rows,
        ));
        if execution.row_count == 1 {
            lines.push("Returned 1 row.".to_string());
        } else {
            lines.push(format!("Returned {} rows.", execution.row_count));
        }
        if execution.truncated {
            lines.push(format!(
                "Output truncated to the first {QUERY_CONSOLE_MAX_ROWS} rows."
            ));
        }
    } else if execution.rows_affected > 0 {
        if execution.rows_affected == 1 {
            lines.push(String::new());
            lines.push("Affected 1 row.".to_string());
        } else {
            lines.push(String::new());
            lines.push(format!("Affected {} rows.", execution.rows_affected));
        }
    } else {
        lines.push(String::new());
        lines.push("Statement completed successfully.".to_string());
    }

    lines.join("\n")
}

fn format_query_console_statement_error(
    statement_number: usize,
    statement: &str,
    error_message: &str,
) -> String {
    [
        format!("Statement {statement_number}"),
        statement_preview_text(statement),
        String::new(),
        format!("Error: {error_message}"),
    ]
    .join("\n")
}

fn format_query_console_result_table(
    column_names: &[String],
    rows: &[QueryConsoleResultRow],
) -> String {
    let mut widths = column_names
        .iter()
        .map(|column_name| column_name.chars().count())
        .collect::<Vec<_>>();

    for row in rows {
        for (column_index, cell) in row.cells.iter().enumerate() {
            if let Some(width) = widths.get_mut(column_index) {
                *width = (*width).max(cell.display_text.chars().count());
            }
        }
    }

    let header = format!(
        "| {} |",
        column_names
            .iter()
            .enumerate()
            .map(|(column_index, column_name)| {
                pad_query_console_cell(column_name, widths[column_index])
            })
            .collect::<Vec<_>>()
            .join(" | ")
    );
    let separator = format!(
        "| {} |",
        widths
            .iter()
            .map(|width| "-".repeat(*width))
            .collect::<Vec<_>>()
            .join(" | ")
    );

    let mut lines = vec![header, separator];
    for row in rows {
        let mut padded_row = Vec::with_capacity(widths.len());
        for (column_index, width) in widths.iter().enumerate() {
            let cell = row
                .cells
                .get(column_index)
                .map(|cell| cell.display_text.clone())
                .unwrap_or_default();
            padded_row.push(pad_query_console_cell(&cell, *width));
        }
        lines.push(format!("| {} |", padded_row.join(" | ")));
    }
    lines.join("\n")
}

fn pad_query_console_cell(value: &str, width: usize) -> String {
    let value_width = value.chars().count();
    let padding = width.saturating_sub(value_width);
    format!("{value}{}", " ".repeat(padding))
}

fn statement_preview_text(statement: &str) -> String {
    let compact = trim_leading_sql_comments_and_whitespace(statement)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if compact.chars().count() <= 100 {
        compact
    } else {
        let preview: String = compact.chars().take(97).collect();
        format!("{preview}...")
    }
}

fn split_query_console_statements(text: &str) -> Vec<ParsedQueryStatement> {
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum ParseState {
        Normal,
        SingleQuoted,
        DoubleQuoted,
        BacktickQuoted,
        LineComment,
        BlockComment,
        DollarQuoted(String),
    }

    let mut statements = Vec::new();
    let mut state = ParseState::Normal;
    let mut segment_start_byte = 0;
    let mut segment_start_utf16 = 0;
    let mut offset_utf16 = 0;
    let mut byte_index = 0;
    let mut has_code = false;

    while byte_index < text.len() {
        let Some(character) = text[byte_index..].chars().next() else {
            break;
        };
        let character_len = character.len_utf8();
        let character_utf16_len = character.len_utf16();

        match &mut state {
            ParseState::Normal => {
                if character == ';' {
                    if has_code {
                        statements.push(ParsedQueryStatement {
                            sql: text[segment_start_byte..byte_index].to_string(),
                            start_utf16: segment_start_utf16,
                            end_utf16: offset_utf16,
                        });
                    }

                    byte_index += character_len;
                    offset_utf16 += character_utf16_len;
                    segment_start_byte = byte_index;
                    segment_start_utf16 = offset_utf16;
                    has_code = false;
                    continue;
                }

                if character == '\'' {
                    has_code = true;
                    state = ParseState::SingleQuoted;
                } else if character == '"' {
                    has_code = true;
                    state = ParseState::DoubleQuoted;
                } else if character == '`' {
                    has_code = true;
                    state = ParseState::BacktickQuoted;
                } else if character == '-' && is_sql_dash_comment_start(text, byte_index) {
                    state = ParseState::LineComment;
                    byte_index += 2;
                    offset_utf16 += 2;
                    continue;
                } else if character == '#' {
                    state = ParseState::LineComment;
                } else if character == '/' && text[byte_index + character_len..].starts_with('*') {
                    state = ParseState::BlockComment;
                    byte_index += 2;
                    offset_utf16 += 2;
                    continue;
                } else if character == '$' {
                    if let Some((delimiter, delimiter_bytes, delimiter_utf16)) =
                        parse_dollar_quote_delimiter(&text[byte_index..])
                    {
                        has_code = true;
                        state = ParseState::DollarQuoted(delimiter);
                        byte_index += delimiter_bytes;
                        offset_utf16 += delimiter_utf16;
                        continue;
                    }
                    has_code |= !character.is_whitespace();
                } else {
                    has_code |= !character.is_whitespace();
                }
            }
            ParseState::SingleQuoted => {
                if character == '\\' {
                    if let Some(next_character) = text[byte_index + character_len..].chars().next()
                    {
                        byte_index += character_len + next_character.len_utf8();
                        offset_utf16 += character_utf16_len + next_character.len_utf16();
                        continue;
                    }
                } else if character == '\'' {
                    if text[byte_index + character_len..].starts_with('\'') {
                        byte_index += 2;
                        offset_utf16 += 2;
                        continue;
                    }
                    state = ParseState::Normal;
                }
            }
            ParseState::DoubleQuoted => {
                if character == '"' {
                    if text[byte_index + character_len..].starts_with('"') {
                        byte_index += 2;
                        offset_utf16 += 2;
                        continue;
                    }
                    state = ParseState::Normal;
                }
            }
            ParseState::BacktickQuoted => {
                if character == '`' {
                    if text[byte_index + character_len..].starts_with('`') {
                        byte_index += 2;
                        offset_utf16 += 2;
                        continue;
                    }
                    state = ParseState::Normal;
                }
            }
            ParseState::LineComment => {
                if character == '\n' {
                    state = ParseState::Normal;
                }
            }
            ParseState::BlockComment => {
                if character == '*' && text[byte_index + character_len..].starts_with('/') {
                    state = ParseState::Normal;
                    byte_index += 2;
                    offset_utf16 += 2;
                    continue;
                }
            }
            ParseState::DollarQuoted(delimiter) => {
                if text[byte_index..].starts_with(delimiter.as_str()) {
                    byte_index += delimiter.len();
                    offset_utf16 += delimiter.encode_utf16().count();
                    state = ParseState::Normal;
                    continue;
                }
            }
        }

        byte_index += character_len;
        offset_utf16 += character_utf16_len;
    }

    if has_code {
        statements.push(ParsedQueryStatement {
            sql: text[segment_start_byte..].to_string(),
            start_utf16: segment_start_utf16,
            end_utf16: offset_utf16,
        });
    }

    statements
}

fn current_query_console_statement(
    statements: &[ParsedQueryStatement],
    cursor_utf16: usize,
) -> Option<&ParsedQueryStatement> {
    for statement in statements {
        if cursor_utf16 >= statement.start_utf16 && cursor_utf16 <= statement.end_utf16 {
            return Some(statement);
        }
        if cursor_utf16 < statement.start_utf16 {
            return Some(statement);
        }
    }
    statements.last()
}

fn utf16_range_to_string_slice(text: &str, range: Range<usize>) -> Option<&str> {
    let byte_range = utf16_range_to_byte_range(text, range)?;
    text.get(byte_range)
}

fn utf16_range_to_byte_range(text: &str, range: Range<usize>) -> Option<Range<usize>> {
    let start = byte_index_for_utf16_offset(text, range.start)?;
    let end = byte_index_for_utf16_offset(text, range.end)?;
    Some(start..end)
}

fn byte_index_for_utf16_offset(text: &str, target_utf16_offset: usize) -> Option<usize> {
    if target_utf16_offset == 0 {
        return Some(0);
    }

    let mut current_utf16_offset = 0;
    for (byte_index, character) in text.char_indices() {
        if current_utf16_offset == target_utf16_offset {
            return Some(byte_index);
        }
        current_utf16_offset += character.len_utf16();
        if current_utf16_offset == target_utf16_offset {
            return Some(byte_index + character.len_utf8());
        }
        if current_utf16_offset > target_utf16_offset {
            return None;
        }
    }

    if current_utf16_offset == target_utf16_offset {
        Some(text.len())
    } else {
        None
    }
}

fn is_sql_dash_comment_start(text: &str, byte_index: usize) -> bool {
    if !text[byte_index..].starts_with("--") {
        return false;
    }

    text[byte_index + 2..]
        .chars()
        .next()
        .is_none_or(|character| character.is_whitespace())
}

fn parse_dollar_quote_delimiter(text: &str) -> Option<(String, usize, usize)> {
    if !text.starts_with('$') {
        return None;
    }

    for (relative_byte_index, character) in text[1..].char_indices() {
        let delimiter_end = relative_byte_index + 1;
        if character == '$' {
            let tag = &text[1..delimiter_end];
            if tag
                .chars()
                .all(|character| character == '_' || character.is_ascii_alphanumeric())
            {
                let delimiter = text[..delimiter_end + 1].to_string();
                let delimiter_utf16 = delimiter.encode_utf16().count();
                return Some((delimiter, delimiter_end + 1, delimiter_utf16));
            }
            return None;
        }

        if !(character == '_' || character.is_ascii_alphanumeric()) {
            return None;
        }
    }

    None
}

fn statement_likely_returns_rows(statement: &str) -> bool {
    let Some(keyword) = leading_sql_keyword(statement) else {
        return false;
    };

    matches!(
        keyword.as_str(),
        "CALL"
            | "DESC"
            | "DESCRIBE"
            | "EXPLAIN"
            | "PRAGMA"
            | "SELECT"
            | "SHOW"
            | "TABLE"
            | "VALUES"
            | "WITH"
    )
}

fn leading_sql_keyword(statement: &str) -> Option<String> {
    let trimmed = trim_leading_sql_comments_and_whitespace(statement);
    let keyword = trimmed
        .chars()
        .take_while(|character| character.is_ascii_alphabetic())
        .collect::<String>();
    if keyword.is_empty() {
        None
    } else {
        Some(keyword.to_ascii_uppercase())
    }
}

fn trim_leading_sql_comments_and_whitespace(mut statement: &str) -> &str {
    loop {
        statement = statement.trim_start();
        if statement.is_empty() {
            return statement;
        }

        if statement.starts_with("--") {
            if let Some(newline_index) = statement.find('\n') {
                statement = &statement[newline_index + 1..];
                continue;
            }
            return "";
        }

        if statement.starts_with('#') {
            if let Some(newline_index) = statement.find('\n') {
                statement = &statement[newline_index + 1..];
                continue;
            }
            return "";
        }

        if statement.starts_with("/*") {
            if let Some(comment_end) = statement.find("*/") {
                statement = &statement[comment_end + 2..];
                continue;
            }
            return "";
        }

        return statement;
    }
}

fn query_console_placeholder_text(
    connection_kind: DatabaseKind,
    schema_name: Option<&str>,
) -> String {
    match schema_name {
        Some(schema_name) => {
            format!(
                "Write SQL for {} {} here...",
                connection_kind.schema_label(),
                schema_name
            )
        }
        None => "Write SQL here...".to_string(),
    }
}

fn query_console_ready_message(schema_name: Option<&str>) -> String {
    match schema_name {
        Some(schema_name) => format!("Ready in {schema_name}."),
        None => "Ready.".to_string(),
    }
}

fn query_console_context_text(
    connection: &SavedDatabaseConnection,
    schema_name: Option<&str>,
) -> String {
    match schema_name {
        Some(schema_name) => format!(
            "{} connection: {} ({})",
            connection.kind.label(),
            connection.name,
            schema_name
        ),
        None => format!(
            "{} connection: {}",
            connection.kind.label(),
            connection.name
        ),
    }
}

fn notice_kind_color(kind: NoticeKind) -> Color {
    match kind {
        NoticeKind::Error => Color::Error,
        NoticeKind::Info => Color::Muted,
        NoticeKind::Success => Color::Success,
    }
}

fn default_query_console_table_text(
    connection_kind: DatabaseKind,
    schema_name: &str,
    table_name: &str,
) -> String {
    let qualified_table_name = match connection_kind {
        DatabaseKind::MySql => format_mysql_qualified_name(schema_name, table_name),
        DatabaseKind::Postgres => format!(
            "{}.{}",
            quote_postgres_identifier(schema_name),
            quote_postgres_identifier(table_name)
        ),
    };

    format!("SELECT *\nFROM {qualified_table_name}\nLIMIT 200;")
}

fn render_database_fill_editor<T>(editor: Entity<Editor>, cx: &mut Context<T>) -> impl IntoElement {
    div()
        .flex_1()
        .min_h(px(0.))
        .px_2()
        .py_2()
        .rounded_md()
        .border_1()
        .border_color(cx.theme().colors().border)
        .bg(cx.theme().colors().editor_background)
        .child(AnyView::from(editor).cached(StyleRefinement::default().v_flex().size_full()))
}

pub struct DatabaseTableEditorModal {
    panel: WeakEntity<DatabasePanel>,
    connection_id: Uuid,
    connection: SavedDatabaseConnection,
    schema_name: String,
    table_name: String,
    name_editor: Entity<Editor>,
    comment_editor: Entity<Editor>,
    engine_editor: Entity<Editor>,
    collation_editor: Entity<Editor>,
    preview_editor: Entity<Editor>,
    details: Option<DatabaseTableDetails>,
    status: TableEditorStatus,
    error_message: Option<String>,
}

impl DatabaseTableEditorModal {
    fn new(
        panel: WeakEntity<DatabasePanel>,
        connection_id: Uuid,
        connection: SavedDatabaseConnection,
        schema_name: String,
        table_name: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let name_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text(&table_name, window, cx);
            editor
        });
        let comment_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("Table comment", window, cx);
            editor
        });
        let engine_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("InnoDB", window, cx);
            editor
        });
        let collation_editor = cx.new(|cx| {
            let mut editor = Editor::single_line(window, cx);
            editor.set_placeholder_text("utf8mb4_0900_ai_ci", window, cx);
            editor
        });
        let preview_editor = cx.new(|cx| {
            let mut editor = Editor::multi_line(window, cx);
            editor.hide_minimap_by_default(window, cx);
            editor.set_show_git_diff_gutter(false, cx);
            editor.set_show_runnables(false, cx);
            editor.set_show_breakpoints(false, cx);
            editor.set_show_edit_predictions(Some(false), window, cx);
            editor.set_read_only(true);
            editor.set_text("Loading table details…", window, cx);
            editor
        });

        cx.subscribe(&name_editor, |this, _, event: &EditorEvent, cx| {
            if matches!(event, EditorEvent::BufferEdited) {
                this.refresh_preview(cx);
            }
        })
        .detach();
        cx.subscribe(&comment_editor, |this, _, event: &EditorEvent, cx| {
            if matches!(event, EditorEvent::BufferEdited) {
                this.refresh_preview(cx);
            }
        })
        .detach();
        cx.subscribe(&engine_editor, |this, _, event: &EditorEvent, cx| {
            if matches!(event, EditorEvent::BufferEdited) {
                this.refresh_preview(cx);
            }
        })
        .detach();
        cx.subscribe(&collation_editor, |this, _, event: &EditorEvent, cx| {
            if matches!(event, EditorEvent::BufferEdited) {
                this.refresh_preview(cx);
            }
        })
        .detach();

        let mut modal = Self {
            panel,
            connection_id,
            connection,
            schema_name,
            table_name,
            name_editor,
            comment_editor,
            engine_editor,
            collation_editor,
            preview_editor,
            details: None,
            status: TableEditorStatus::Loading,
            error_message: None,
        };
        modal.load_details(cx);
        modal
    }

    fn load_details(&mut self, cx: &mut Context<Self>) {
        self.status = TableEditorStatus::Loading;
        self.error_message = None;
        self.refresh_preview(cx);

        let connection_kind = self.connection.kind;
        let connection_name = self.connection.name.clone();
        let connection_url = self.connection.url.clone();
        let connection_url_for_task = connection_url.clone();
        let schema_name = self.schema_name.clone();
        let schema_name_for_task = schema_name.clone();
        let table_name = self.table_name.clone();
        let table_name_for_task = table_name.clone();
        cx.spawn(async move |this, cx| -> Result<()> {
            match Tokio::spawn_result(cx, async move {
                load_table_editor_details(
                    connection_kind,
                    connection_url_for_task,
                    schema_name_for_task,
                    table_name_for_task,
                )
                .await
            })
            .await
            {
                Ok(details) => {
                    let loaded_table_name = details.table_name.clone();
                    let loaded_engine = details.engine.clone();
                    let loaded_collation = details.collation.clone();
                    let loaded_comment = details.comment.clone();
                    this.update(cx, |this, cx| {
                        this.table_name = loaded_table_name.clone();
                        this.details = Some(details);
                        this.status = TableEditorStatus::Ready;
                        this.error_message = None;
                        set_editor_text(&this.name_editor, loaded_table_name, cx);
                        set_editor_text(&this.engine_editor, loaded_engine, cx);
                        set_editor_text(&this.collation_editor, loaded_collation, cx);
                        set_editor_text(&this.comment_editor, loaded_comment, cx);
                        this.refresh_preview(cx);
                        cx.notify();
                    })?;
                }
                Err(error) => {
                    let error_message = format_connection_error(&error, &connection_url);
                    log::error!(
                        "Failed to load table editor details for {}.{} on {}: {}",
                        schema_name,
                        table_name,
                        connection_name,
                        error_message
                    );
                    this.update(cx, |this, cx| {
                        this.details = None;
                        this.status = TableEditorStatus::Ready;
                        this.error_message = Some(error_message.clone());
                        set_editor_text(
                            &this.preview_editor,
                            format!("Could not load table details.\n\n{error_message}"),
                            cx,
                        );
                        cx.notify();
                    })?;
                }
            }

            Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn current_values(&self, cx: &App) -> TableEditorValues {
        TableEditorValues {
            table_name: self.name_editor.read(cx).text(cx).trim().to_string(),
            engine: self.engine_editor.read(cx).text(cx).trim().to_string(),
            collation: self.collation_editor.read(cx).text(cx).trim().to_string(),
            comment: self.comment_editor.read(cx).text(cx).to_string(),
        }
    }

    fn refresh_preview(&mut self, cx: &mut Context<Self>) {
        let preview_text =
            if matches!(self.status, TableEditorStatus::Loading) && self.details.is_none() {
                "Loading table details…".to_string()
            } else if let Some(details) = &self.details {
                match preview_table_edit(details, &self.current_values(cx)) {
                    Ok(preview) => preview,
                    Err(error) => format!("Preview unavailable.\n\n{error}"),
                }
            } else if let Some(error_message) = &self.error_message {
                format!("Could not load table details.\n\n{error_message}")
            } else {
                "Table details are not available.".to_string()
            };

        set_editor_text(&self.preview_editor, preview_text, cx);
        cx.notify();
    }

    fn cancel(&mut self, _: &menu::Cancel, _window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.status, TableEditorStatus::Saving) {
            return;
        }

        cx.emit(DismissEvent);
    }

    fn confirm(&mut self, _: &menu::Confirm, _window: &mut Window, cx: &mut Context<Self>) {
        self.save_changes(cx);
    }

    fn save_changes(&mut self, cx: &mut Context<Self>) {
        if !matches!(self.status, TableEditorStatus::Ready) {
            return;
        }

        let Some(details) = self.details.clone() else {
            self.error_message = Some("Table details are still loading.".to_string());
            self.refresh_preview(cx);
            return;
        };

        let values = self.current_values(cx);
        let plan = match details.kind {
            DatabaseKind::Postgres => {
                Err(anyhow!("Table editing is not supported yet for Postgres."))
            }
            DatabaseKind::MySql => build_mysql_table_edit_plan(&details, &values),
        };

        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => {
                self.error_message = Some(error.to_string());
                self.refresh_preview(cx);
                return;
            }
        };

        if plan.is_empty() {
            cx.emit(DismissEvent);
            return;
        }

        self.status = TableEditorStatus::Saving;
        self.error_message = None;
        self.refresh_preview(cx);

        let panel = self.panel.clone();
        let connection_kind = self.connection.kind;
        let connection_name = self.connection.name.clone();
        let connection_url = self.connection.url.clone();
        let connection_url_for_task = connection_url.clone();
        let connection_id = self.connection_id;
        let schema_name = details.schema_name.clone();
        let previous_table_name = details.table_name.clone();
        let target_table_name = plan.target_table_name.clone();
        cx.spawn(async move |this, cx| -> Result<()> {
            match Tokio::spawn_result(cx, async move {
                apply_table_edit(connection_kind, connection_url_for_task, plan).await
            })
            .await
            {
                Ok(()) => {
                    let panel_message = if previous_table_name == target_table_name {
                        format!("Updated table {}.{}. Refreshing metadata…", schema_name, target_table_name)
                    } else {
                        format!(
                            "Updated table {}.{} -> {}. Refreshing metadata…",
                            schema_name, previous_table_name, target_table_name
                        )
                    };
                    if let Err(error) = panel.update(cx, |panel, cx| {
                        panel.refresh_connection(connection_id, cx);
                        panel.notice = Some(PanelNotice {
                            kind: NoticeKind::Success,
                            message: panel_message.clone(),
                        });
                        cx.notify();
                    }) {
                        log::error!(
                            "Failed to refresh database panel after updating table {}.{}: {error:#}",
                            schema_name,
                            target_table_name
                        );
                    }
                    this.update(cx, |this, cx| {
                        this.status = TableEditorStatus::Ready;
                        this.error_message = None;
                        cx.emit(DismissEvent);
                    })?;
                }
                Err(error) => {
                    let error_message = format_connection_error(&error, &connection_url);
                    log::error!(
                        "Failed to update table {}.{} on {}: {}",
                        schema_name,
                        previous_table_name,
                        connection_name,
                        error_message
                    );
                    this.update(cx, |this, cx| {
                        this.status = TableEditorStatus::Ready;
                        this.error_message = Some(error_message);
                        this.refresh_preview(cx);
                    })?;
                }
            }

            Ok(())
        })
        .detach_and_log_err(cx);
    }

    fn render_columns(&self, cx: &mut Context<Self>) -> AnyElement {
        if let Some(details) = &self.details {
            if details.columns.is_empty() {
                return Label::new("No columns were returned for this table.")
                    .size(LabelSize::Small)
                    .color(Color::Muted)
                    .into_any_element();
            }

            return v_flex()
                .w_full()
                .gap_1()
                .children(details.columns.iter().map(|column| {
                    v_flex()
                        .w_full()
                        .gap_0p5()
                        .px_2()
                        .py_1p5()
                        .rounded_sm()
                        .border_1()
                        .border_color(cx.theme().colors().border_variant)
                        .bg(cx.theme().colors().editor_background)
                        .child(Label::new(column.name.clone()).size(LabelSize::Small))
                        .child(
                            Label::new(column.detail.clone())
                                .size(LabelSize::Small)
                                .color(Color::Muted),
                        )
                }))
                .into_any_element();
        }

        let message = if matches!(self.status, TableEditorStatus::Loading) {
            "Loading columns…"
        } else {
            "Columns will appear once the table definition is loaded."
        };
        Label::new(message)
            .size(LabelSize::Small)
            .color(Color::Muted)
            .into_any_element()
    }

    fn render_status(&self) -> AnyElement {
        let (message, color) = if let Some(error_message) = &self.error_message {
            (error_message.clone(), Color::Error)
        } else if matches!(self.status, TableEditorStatus::Loading) {
            ("Loading table details…".to_string(), Color::Muted)
        } else if matches!(self.status, TableEditorStatus::Saving) {
            ("Applying table changes…".to_string(), Color::Muted)
        } else {
            (
                "Press OK to apply the generated DDL.".to_string(),
                Color::Muted,
            )
        };

        Label::new(message)
            .size(LabelSize::Small)
            .color(color)
            .into_any_element()
    }
}

impl EventEmitter<DismissEvent> for DatabaseTableEditorModal {}
impl ModalView for DatabaseTableEditorModal {}

impl Focusable for DatabaseTableEditorModal {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.name_editor.focus_handle(cx)
    }
}

impl Render for DatabaseTableEditorModal {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let create_options = self
            .details
            .as_ref()
            .map(|details| details.create_options.clone())
            .filter(|create_options| !create_options.is_empty());
        let header_description = format!(
            "{} connection · {}.{}",
            self.connection.kind.label(),
            self.schema_name,
            self.table_name
        );

        v_flex()
            .w(px(860.))
            .max_h(px(720.))
            .elevation_3(cx)
            .on_action(cx.listener(Self::cancel))
            .on_action(cx.listener(Self::confirm))
            .child(
                Modal::new("database-table-editor-modal", None)
                    .show_dismiss(true)
                    .header(
                        ModalHeader::new()
                            .headline("Modify Table")
                            .description(header_description),
                    )
                    .section(
                        Section::new().child(
                            v_flex()
                                .gap_4()
                                .child(
                                    h_flex()
                                        .w_full()
                                        .items_start()
                                        .gap_4()
                                        .child(
                                            v_flex()
                                                .w(px(260.))
                                                .gap_2()
                                                .p_3()
                                                .rounded_md()
                                                .border_1()
                                                .border_color(cx.theme().colors().border_variant)
                                                .bg(cx.theme().colors().panel_background)
                                                .child(SectionHeader::new("Columns"))
                                                .child(
                                                    div()
                                                        .id("database-table-editor-columns-scroll")
                                                        .w_full()
                                                        .max_h(px(260.))
                                                        .overflow_y_scroll()
                                                        .child(
                                                            v_flex()
                                                                .w_full()
                                                                .gap_1()
                                                                .child(self.render_columns(cx)),
                                                        ),
                                                ),
                                        )
                                        .child(
                                            v_flex()
                                                .flex_1()
                                                .min_w_0()
                                                .gap_3()
                                                .p_3()
                                                .rounded_md()
                                                .border_1()
                                                .border_color(cx.theme().colors().border_variant)
                                                .bg(cx.theme().colors().panel_background)
                                                .child(SectionHeader::new("Properties"))
                                                .child(
                                                    h_flex()
                                                        .w_full()
                                                        .gap_2()
                                                        .child(v_flex().w(px(180.)).child(
                                                            render_database_read_only_field(
                                                                "Schema",
                                                                self.schema_name.clone(),
                                                                cx,
                                                            ),
                                                        ))
                                                        .child(v_flex().flex_1().child(
                                                            render_database_field(
                                                                "Table Name",
                                                                self.name_editor.clone(),
                                                                cx,
                                                            ),
                                                        )),
                                                )
                                                .child(
                                                    h_flex()
                                                        .w_full()
                                                        .gap_2()
                                                        .child(v_flex().flex_1().child(
                                                            render_database_field(
                                                                "Engine",
                                                                self.engine_editor.clone(),
                                                                cx,
                                                            ),
                                                        ))
                                                        .child(v_flex().flex_1().child(
                                                            render_database_field(
                                                                "Collation",
                                                                self.collation_editor.clone(),
                                                                cx,
                                                            ),
                                                        )),
                                                )
                                                .child(render_database_field(
                                                    "Comment",
                                                    self.comment_editor.clone(),
                                                    cx,
                                                ))
                                                .when_some(
                                                    create_options,
                                                    |this, create_options| {
                                                        this.child(render_database_read_only_field(
                                                            "Create Options",
                                                            create_options,
                                                            cx,
                                                        ))
                                                    },
                                                ),
                                        ),
                                )
                                .child(
                                    v_flex()
                                        .gap_2()
                                        .p_3()
                                        .rounded_md()
                                        .border_1()
                                        .border_color(cx.theme().colors().border_variant)
                                        .bg(cx.theme().colors().panel_background)
                                        .child(SectionHeader::new("DDL Preview"))
                                        .child(render_database_multiline_input(
                                            self.preview_editor.clone(),
                                            px(280.),
                                            cx,
                                        )),
                                ),
                        ),
                    )
                    .footer(
                        ModalFooter::new()
                            .start_slot(self.render_status())
                            .end_slot(
                                h_flex()
                                    .gap_2()
                                    .child(
                                        Button::new("cancel-table-editor", "Cancel")
                                            .style(ButtonStyle::Subtle)
                                            .disabled(matches!(
                                                self.status,
                                                TableEditorStatus::Saving
                                            ))
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.cancel(&menu::Cancel, window, cx);
                                            })),
                                    )
                                    .child(
                                        Button::new("save-table-editor", "OK")
                                            .style(ButtonStyle::Filled)
                                            .disabled(
                                                !matches!(self.status, TableEditorStatus::Ready)
                                                    || self.details.is_none(),
                                            )
                                            .on_click(cx.listener(|this, _, window, cx| {
                                                this.confirm(&menu::Confirm, window, cx);
                                            })),
                                    ),
                            ),
                    ),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::TestAppContext;

    fn sample_mysql_table_details() -> DatabaseTableDetails {
        DatabaseTableDetails {
            kind: DatabaseKind::MySql,
            schema_name: "main_db".to_string(),
            table_name: "users".to_string(),
            engine: "InnoDB".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
            comment: "Users table".to_string(),
            create_options: String::new(),
            create_table_ddl: "CREATE TABLE `users` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB DEFAULT COLLATE=utf8mb4_general_ci COMMENT='Users table'".to_string(),
            columns: Vec::new(),
        }
    }

    fn sample_query_console_result_table() -> QueryConsoleResultTable {
        QueryConsoleResultTable {
            statement: "SELECT * FROM `main_db`.`users`;".to_string(),
            column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
            rows: vec![QueryConsoleResultRow {
                cells: vec![
                    query_console_cell_value(QueryConsoleSqlValue::Number("42".to_string())),
                    query_console_cell_value(QueryConsoleSqlValue::Text("Alice".to_string())),
                    query_console_cell_value(QueryConsoleSqlValue::Null),
                ],
            }],
            row_count: 1,
            truncated: false,
            mutation_metadata: Some(QueryConsoleTableMutationMetadata {
                table_path: DatabaseTablePath::new("main_db", "users"),
                table_column_names: vec!["id".to_string(), "name".to_string(), "email".to_string()],
                key_column_names: vec!["id".to_string()],
            }),
            mutation_message: None,
        }
    }

    #[gpui::test]
    fn query_console_result_table_has_scrollable_overflow(cx: &mut TestAppContext) {
        cx.update(|cx| {
            settings::init(cx);
            theme_settings::init(theme::LoadThemes::JustBase, cx);
        });

        let connection = SavedDatabaseConnection {
            id: Uuid::nil(),
            name: "Test Connection".to_string(),
            url: "mysql://localhost:3306/main_db".to_string(),
            kind: DatabaseKind::MySql,
        };
        let mut large_result_table = sample_query_console_result_table();
        large_result_table.column_names = vec![
            "id".to_string(),
            "name".to_string(),
            "email".to_string(),
            "notes".to_string(),
            "metadata".to_string(),
        ];
        large_result_table.rows = (0..40)
            .map(|row_index| QueryConsoleResultRow {
                cells: vec![
                    query_console_cell_value(QueryConsoleSqlValue::Number(row_index.to_string())),
                    query_console_cell_value(QueryConsoleSqlValue::Text(format!(
                        "User {row_index}"
                    ))),
                    query_console_cell_value(QueryConsoleSqlValue::Text(format!(
                        "user{row_index}@example.com"
                    ))),
                    query_console_cell_value(QueryConsoleSqlValue::Text(
                        "A very long note that should force horizontal overflow in the results panel."
                            .to_string(),
                    )),
                    query_console_cell_value(QueryConsoleSqlValue::Text(
                        "{\"role\":\"admin\",\"active\":true}".to_string(),
                    )),
                ],
            })
            .collect();
        large_result_table.row_count = large_result_table.rows.len();
        large_result_table.mutation_metadata = Some(QueryConsoleTableMutationMetadata {
            table_path: DatabaseTablePath::new("main_db", "users"),
            table_column_names: large_result_table.column_names.clone(),
            key_column_names: vec!["id".to_string()],
        });

        let expected_table_width = query_console_result_table_width(&large_result_table);
        let (query_console, cx) = cx.add_window_view({
            let connection = connection.clone();
            move |window, cx| {
                DatabaseQueryConsole::new(connection.clone(), None, None, None, window, cx)
            }
        });

        query_console.update(cx, |query_console, cx| {
            query_console.results_panel_height = px(180.);
            query_console.set_result_table(Some(large_result_table), cx);
            cx.notify();
        });
        cx.simulate_resize(gpui::size(px(320.), px(260.)));
        cx.run_until_parked();

        let (
            horizontal_offset,
            horizontal_viewport_width,
            vertical_offset,
            vertical_viewport_height,
        ) = query_console.read_with(cx, |query_console, _| {
            (
                query_console.result_table_scroll_handle.max_offset().x,
                query_console.result_table_scroll_handle.bounds().size.width,
                query_console.result_table_scroll_handle.max_offset().y,
                query_console
                    .result_table_scroll_handle
                    .bounds()
                    .size
                    .height,
            )
        });

        assert!(
            horizontal_offset > Pixels::ZERO,
            "expected horizontal overflow, got {horizontal_offset:?} with viewport width {horizontal_viewport_width:?} and table width {expected_table_width:?}"
        );
        assert!(
            vertical_offset > Pixels::ZERO,
            "expected vertical overflow, got {vertical_offset:?} with viewport height {vertical_viewport_height:?}"
        );
    }

    #[test]
    fn query_console_result_table_uses_compact_content_widths() {
        let result_table = sample_query_console_result_table();
        let widths = query_console_result_table_data_column_widths(&result_table);
        let total_width = query_console_result_table_width(&result_table);

        assert_eq!(widths.len(), 3);
        assert_eq!(widths[0], QUERY_CONSOLE_RESULT_COLUMN_MIN_WIDTH);
        assert!(
            widths[0] < widths[2],
            "expected the id column to stay narrower than the action column: {widths:?}"
        );
        assert!(
            total_width < px(800.),
            "expected compact widths to stay below the previous fixed layout, got {total_width:?}"
        );
    }

    #[test]
    fn build_connection_url_encodes_special_password_characters() {
        let connection_form = DatabaseConnectionForm {
            kind: DatabaseKind::MySql,
            host: "91.98.91.27".to_string(),
            port: "3306".to_string(),
            user: "my_personal_user".to_string(),
            password: "x.:paA^qToVju_^8Y<H(".to_string(),
            database_name: String::new(),
            options: String::new(),
        };

        let (connection_url, parsed_url) =
            build_connection_url(&connection_form).expect("connection url should be valid");

        assert_eq!(
            connection_url,
            "mysql://my_personal_user:x.%3ApaA%5EqToVju_%5E8Y%3CH%28@91.98.91.27:3306"
        );
        assert_eq!(parsed_url.host_str(), Some("91.98.91.27"));
        assert_eq!(parsed_url.port(), Some(3306));
    }

    #[test]
    fn connection_form_from_saved_connection_decodes_saved_values() {
        let saved_connection = SavedDatabaseConnection {
            id: Uuid::nil(),
            name: "Production".to_string(),
            url: "mysql://my_personal_user:x.%3ApaA%5EqToVju_%5E8Y%3CH%28@91.98.91.27:3306/main_db?ssl-mode=REQUIRED".to_string(),
            kind: DatabaseKind::MySql,
        };

        let connection_form = connection_form_from_saved_connection(&saved_connection)
            .expect("saved connection should deserialize into the form");

        assert_eq!(connection_form.kind, DatabaseKind::MySql);
        assert_eq!(connection_form.host, "91.98.91.27");
        assert_eq!(connection_form.port, "3306");
        assert_eq!(connection_form.user, "my_personal_user");
        assert_eq!(connection_form.password, "x.:paA^qToVju_^8Y<H(");
        assert_eq!(connection_form.database_name, "main_db");
        assert_eq!(connection_form.options, "ssl-mode=REQUIRED");
    }

    #[test]
    fn normalize_saved_connection_rewrites_legacy_raw_credentials() {
        let saved_connection = SavedDatabaseConnection {
            id: Uuid::nil(),
            name: "Production".to_string(),
            url: "mysql://my_personal_user:x.:paA^qToVju_^8Y<H(@91.98.91.27:3306".to_string(),
            kind: DatabaseKind::MySql,
        };

        let normalized_connection = normalize_saved_connection(saved_connection)
            .expect("legacy saved connection should normalize successfully");

        assert_eq!(
            normalized_connection.url,
            "mysql://my_personal_user:x.%3ApaA%5EqToVju_%5E8Y%3CH%28@91.98.91.27:3306"
        );
    }

    #[test]
    fn selected_mysql_schema_name_decodes_database_name() {
        let schema_name =
            selected_mysql_schema_name("mysql://user:password@localhost:3306/main_db%2Farchive")
                .expect("database name should parse");

        assert_eq!(schema_name, Some("main_db/archive".to_string()));
    }

    #[test]
    fn format_connection_error_includes_chain_and_sanitizes_connection_url() {
        let connection_url = "mysql://user:password@localhost:3306/main_db";
        let error =
            anyhow!("Access denied for {}", connection_url).context("Could not load schemas");

        assert_eq!(
            format_connection_error(&error, connection_url),
            "Could not load schemas: Access denied for <connection-url>"
        );
    }

    #[test]
    fn format_database_table_key_detail_formats_foreign_key_reference() {
        let detail = format_database_table_key_detail(
            "FOREIGN KEY",
            "user_id",
            Some("main_db.users"),
            Some("id"),
        );

        assert_eq!(detail, "FOREIGN KEY (user_id) -> main_db.users (id)");
    }

    #[test]
    fn format_database_table_key_detail_handles_missing_column_names() {
        let detail = format_database_table_key_detail("PRIMARY KEY", "", None, None);

        assert_eq!(detail, "PRIMARY KEY (unknown columns)");
    }

    #[test]
    fn preview_mysql_table_edit_returns_original_ddl_when_unchanged() {
        let details = sample_mysql_table_details();
        let values = TableEditorValues {
            table_name: "users".to_string(),
            engine: "InnoDB".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
            comment: "Users table".to_string(),
        };

        let preview = preview_mysql_table_edit(&details, &values)
            .expect("unchanged MySQL table should return original DDL");

        assert_eq!(preview, details.create_table_ddl);
    }

    #[test]
    fn build_mysql_table_edit_plan_generates_rename_and_alter_statements() {
        let details = sample_mysql_table_details();
        let values = TableEditorValues {
            table_name: "customers".to_string(),
            engine: "MyISAM".to_string(),
            collation: "utf8mb4_unicode_ci".to_string(),
            comment: "Customer records".to_string(),
        };

        let plan = build_mysql_table_edit_plan(&details, &values)
            .expect("valid changes should produce a MySQL ALTER plan");

        assert_eq!(
            plan.rename_statement.as_deref(),
            Some("RENAME TABLE `main_db`.`users` TO `main_db`.`customers`")
        );
        assert_eq!(
            plan.alter_statement.as_deref(),
            Some(
                "ALTER TABLE `main_db`.`customers` ENGINE = MyISAM, COLLATE = utf8mb4_unicode_ci, COMMENT = 'Customer records'"
            )
        );
        assert_eq!(plan.target_table_name, "customers");
    }

    #[test]
    fn build_mysql_table_edit_plan_rejects_invalid_engine_tokens() {
        let details = sample_mysql_table_details();
        let values = TableEditorValues {
            table_name: "users".to_string(),
            engine: "InnoDB-Cluster".to_string(),
            collation: "utf8mb4_general_ci".to_string(),
            comment: "Users table".to_string(),
        };

        let error = build_mysql_table_edit_plan(&details, &values)
            .expect_err("invalid engine token should be rejected");

        assert_eq!(
            error.to_string(),
            "Engine can only contain letters, numbers, and underscores."
        );
    }

    #[test]
    fn build_query_console_update_statement_uses_key_columns() {
        let result_table = sample_query_console_result_table();
        let original_row = result_table
            .rows
            .first()
            .expect("sample result should include one row");

        let statement = build_query_console_update_statement(
            DatabaseKind::MySql,
            &result_table,
            original_row,
            &["42".to_string(), "Alicia".to_string(), "".to_string()],
        )
        .expect("update statement should be generated");

        assert_eq!(
            statement,
            "UPDATE `main_db`.`users` SET `name` = 'Alicia' WHERE `id` = 42;"
        );
    }

    #[test]
    fn build_query_console_delete_statement_uses_key_columns() {
        let result_table = sample_query_console_result_table();
        let original_row = result_table
            .rows
            .first()
            .expect("sample result should include one row");

        let statement =
            build_query_console_delete_statement(DatabaseKind::MySql, &result_table, original_row)
                .expect("delete statement should be generated");

        assert_eq!(statement, "DELETE FROM `main_db`.`users` WHERE `id` = 42;");
    }

    #[test]
    fn build_query_console_insert_statement_omits_blank_fields() {
        let result_table = sample_query_console_result_table();

        let statement = build_query_console_insert_statement(
            DatabaseKind::MySql,
            &result_table,
            &["43".to_string(), "Bob".to_string(), "".to_string()],
        )
        .expect("insert statement should be generated");

        assert_eq!(
            statement,
            "INSERT INTO `main_db`.`users` (`id`, `name`) VALUES (43, 'Bob');"
        );
    }

    #[test]
    fn serialized_query_console_ui_state_defaults_missing_fields() {
        let ui_state = serde_json::from_str::<SerializedQueryConsoleUiState>("{}")
            .expect("missing fields should use serde defaults");

        assert_eq!(
            ui_state.results_panel_height,
            default_query_console_results_panel_height_value()
        );
        assert!(!ui_state.summary_expanded);
    }

    #[test]
    fn clamp_query_console_results_panel_height_preserves_sql_space() {
        let clamped_height = clamp_query_console_results_panel_height(px(500.), px(420.));

        assert_eq!(
            clamped_height,
            px(420.)
                - QUERY_CONSOLE_MIN_SQL_PANEL_HEIGHT
                - QUERY_CONSOLE_RESULTS_RESIZE_HANDLE_SIZE
        );
    }

    #[test]
    fn split_query_console_statements_ignores_semicolons_in_comments_and_strings() {
        let sql = r#"
        SELECT ';' AS value;
        -- comment with ;
        /* block comment ; */
        INSERT INTO logs(message) VALUES ('hello; world');
        CREATE FUNCTION test_fn() RETURNS void AS $$
        BEGIN
            RAISE NOTICE ';';
        END;
        $$ LANGUAGE plpgsql;
        "#;

        let statements = split_query_console_statements(sql);

        assert_eq!(statements.len(), 3);
        assert!(statements[0].sql.contains("SELECT ';' AS value"));
        assert!(statements[1].sql.contains("INSERT INTO logs"));
        assert!(statements[2].sql.contains("CREATE FUNCTION test_fn()"));
        assert!(statements[2].sql.contains("RAISE NOTICE ';';"));
    }

    #[test]
    fn current_query_console_statement_uses_next_statement_from_gap() {
        let sql = "SELECT 1;\n\nSELECT 2;\n";
        let statements = split_query_console_statements(sql);
        let gap_byte_offset = sql.find("\n\nSELECT").expect("gap should exist") + 1;
        let gap_utf16_offset = utf16_offset_for_byte(sql, gap_byte_offset);

        let statement = current_query_console_statement(&statements, gap_utf16_offset)
            .expect("cursor in the gap should still resolve to a statement");

        assert!(statement.sql.contains("SELECT 2"));
    }

    #[test]
    fn utf16_range_to_byte_range_handles_multibyte_text() {
        let text = "a🦀b";

        let crab_range = utf16_range_to_byte_range(text, 1..3)
            .expect("emoji range should map to a valid byte range");
        let full_range = utf16_range_to_byte_range(text, 0..4)
            .expect("the full UTF-16 range should map to the full string");

        assert_eq!(&text[crab_range], "🦀");
        assert_eq!(&text[full_range], text);
        assert!(utf16_range_to_byte_range(text, 2..3).is_none());
    }

    #[test]
    fn format_query_console_text_bytes_handles_invalid_utf8() {
        let text = format_query_console_text_bytes(&[b'f', 0xFF, b'g']);

        assert_eq!(text, "f\u{FFFD}g");
    }

    fn utf16_offset_for_byte(text: &str, byte_offset: usize) -> usize {
        text[..byte_offset].encode_utf16().count()
    }
}
