use anyhow::{Context as _, Result, anyhow};
use collections::{HashMap, HashSet};
use theme::AppearanceContent;
use theme_settings::{
    FontStyleContent, FontWeightContent, HighlightStyleContent, ThemeContent, ThemeFamilyContent,
    ThemeStyleContent, WindowBackgroundContent,
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IntelliJTextAttributes {
    pub base_attributes: Option<String>,
    pub foreground: Option<String>,
    pub background: Option<String>,
    pub font_type: Option<u8>,
}

impl IntelliJTextAttributes {
    fn merge_from(&mut self, other: &Self) {
        if other.foreground.is_some() {
            self.foreground = other.foreground.clone();
        }
        if other.background.is_some() {
            self.background = other.background.clone();
        }
        if other.font_type.is_some() {
            self.font_type = other.font_type;
        }
    }

    fn has_explicit_style(&self) -> bool {
        self.foreground.is_some() || self.background.is_some() || self.font_type.is_some()
    }
}

#[derive(Debug, Clone, Default)]
pub struct IntelliJColorScheme {
    pub name: String,
    pub parent_scheme: Option<String>,
    colors: HashMap<String, String>,
    attributes: HashMap<String, IntelliJTextAttributes>,
}

impl IntelliJColorScheme {
    pub fn parse(content: &str) -> Result<Self> {
        let mut scheme = Self::default();

        enum Section {
            Root,
            Colors,
            Attributes,
        }

        let mut section = Section::Root;
        let mut current_attribute_name: Option<String> = None;
        let mut current_attribute = IntelliJTextAttributes::default();

        for raw_line in content.lines() {
            let line = raw_line.trim();

            if line.starts_with("<scheme ") {
                scheme.name = extract_attribute(line, "name")
                    .ok_or_else(|| anyhow!("missing scheme name"))?;
                scheme.parent_scheme = extract_attribute(line, "parent_scheme");
                continue;
            }

            match line {
                "<colors>" => {
                    section = Section::Colors;
                    continue;
                }
                "</colors>" => {
                    section = Section::Root;
                    continue;
                }
                "<attributes>" => {
                    section = Section::Attributes;
                    continue;
                }
                "</attributes>" => {
                    section = Section::Root;
                    continue;
                }
                _ => {}
            }

            match section {
                Section::Root => {}
                Section::Colors => {
                    if line.starts_with("<option ") {
                        let Some(name) = extract_attribute(line, "name") else {
                            continue;
                        };
                        let Some(value) = extract_attribute(line, "value") else {
                            continue;
                        };
                        scheme.colors.insert(name, normalize_hex(&value));
                    }
                }
                Section::Attributes => {
                    if current_attribute_name.is_none() {
                        if !line.starts_with("<option ") {
                            continue;
                        }

                        let Some(name) = extract_attribute(line, "name") else {
                            continue;
                        };

                        current_attribute_name = Some(name);
                        current_attribute = IntelliJTextAttributes {
                            base_attributes: extract_attribute(line, "baseAttributes"),
                            ..Default::default()
                        };

                        if line.ends_with("/>") {
                            let name = current_attribute_name.take().unwrap();
                            scheme.attributes.insert(name, current_attribute.clone());
                            current_attribute = IntelliJTextAttributes::default();
                        }
                    } else if line.starts_with("<option ") {
                        let Some(option_name) = extract_attribute(line, "name") else {
                            continue;
                        };
                        let Some(value) = extract_attribute(line, "value") else {
                            continue;
                        };

                        match option_name.as_str() {
                            "FOREGROUND" => {
                                current_attribute.foreground = Some(normalize_hex(&value))
                            }
                            "BACKGROUND" => {
                                current_attribute.background = Some(normalize_hex(&value))
                            }
                            "FONT_TYPE" => current_attribute.font_type = value.parse().ok(),
                            _ => {}
                        }
                    } else if line == "</option>" {
                        let name = current_attribute_name.take().unwrap();
                        scheme.attributes.insert(name, current_attribute.clone());
                        current_attribute = IntelliJTextAttributes::default();
                    }
                }
            }
        }

        if scheme.name.is_empty() {
            return Err(anyhow!("failed to parse IntelliJ color scheme name"));
        }

        Ok(scheme)
    }

    pub fn color(&self, name: &str) -> Option<String> {
        self.colors.get(name).cloned()
    }

    pub fn apply_to_theme_family(
        &self,
        mut family: ThemeFamilyContent,
    ) -> Result<(ThemeFamilyContent, ThemeStyleContent)> {
        if family.themes.is_empty() {
            family.themes.push(ThemeContent {
                name: self.name.clone(),
                appearance: AppearanceContent::Dark,
                style: ThemeStyleContent {
                    window_background_appearance: Some(WindowBackgroundContent::Opaque),
                    ..Default::default()
                },
            });
        }

        let style = {
            let theme = family
                .themes
                .first_mut()
                .context("base theme family does not contain a theme")?;
            theme.style.window_background_appearance = theme
                .style
                .window_background_appearance
                .or(Some(WindowBackgroundContent::Opaque));
            apply_theme_colors(self, &mut theme.style);
            apply_syntax_colors(self, &mut theme.style);
            theme.style.clone()
        };

        Ok((family, style))
    }

    pub fn into_theme_family(self) -> Result<(ThemeFamilyContent, ThemeStyleContent)> {
        let family = ThemeFamilyContent {
            name: self.name.clone(),
            author: "theme_importer".to_string(),
            themes: vec![ThemeContent {
                name: self.name.clone(),
                appearance: AppearanceContent::Dark,
                style: ThemeStyleContent {
                    window_background_appearance: Some(WindowBackgroundContent::Opaque),
                    ..Default::default()
                },
            }],
        };

        self.apply_to_theme_family(family)
    }

    fn resolved_attribute_with_raw(
        &self,
        name: &str,
    ) -> Option<(IntelliJTextAttributes, IntelliJTextAttributes)> {
        let mut visiting = HashSet::default();
        self.resolve_attribute_inner(name, &mut visiting)
    }

    fn resolve_attribute_inner(
        &self,
        name: &str,
        visiting: &mut HashSet<String>,
    ) -> Option<(IntelliJTextAttributes, IntelliJTextAttributes)> {
        if !visiting.insert(name.to_string()) {
            return None;
        }

        let raw = self.attributes.get(name)?.clone();
        let mut resolved = if let Some(base_name) = raw.base_attributes.as_deref() {
            self.resolve_attribute_inner(base_name, visiting)
                .map(|(resolved, _)| resolved)
                .unwrap_or_default()
        } else {
            IntelliJTextAttributes::default()
        };

        resolved.merge_from(&raw);
        visiting.remove(name);
        Some((resolved, raw))
    }
}

#[derive(Clone, Copy)]
struct SyntaxMapping {
    zed_name: &'static str,
    intellij_names: &'static [&'static str],
    allow_inherited: bool,
}

const SYNTAX_MAPPINGS: &[SyntaxMapping] = &[
    SyntaxMapping {
        zed_name: "boolean",
        intellij_names: &["DEFAULT_NUMBER"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "comment",
        intellij_names: &["DEFAULT_LINE_COMMENT", "DEFAULT_BLOCK_COMMENT"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "comment.doc",
        intellij_names: &["DEFAULT_DOC_COMMENT"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "constant",
        intellij_names: &["DEFAULT_CONSTANT"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "function",
        intellij_names: &["DEFAULT_FUNCTION_DECLARATION"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "function.builtin",
        intellij_names: &["DEFAULT_FUNCTION_DECLARATION"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "function.call",
        intellij_names: &["DEFAULT_FUNCTION_CALL"],
        allow_inherited: false,
    },
    SyntaxMapping {
        zed_name: "function.method",
        intellij_names: &["DEFAULT_INSTANCE_METHOD"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "function.method.call",
        intellij_names: &["DEFAULT_FUNCTION_CALL"],
        allow_inherited: false,
    },
    SyntaxMapping {
        zed_name: "keyword",
        intellij_names: &["DEFAULT_KEYWORD"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "number",
        intellij_names: &["DEFAULT_NUMBER"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "operator",
        intellij_names: &["DEFAULT_OPERATION_SIGN"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "punctuation",
        intellij_names: &["DEFAULT_COMMA"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "punctuation.bracket",
        intellij_names: &["DEFAULT_BRACES", "DEFAULT_BRACKETS", "DEFAULT_PARENTHS"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "punctuation.delimiter",
        intellij_names: &["DEFAULT_COMMA", "DEFAULT_DOT", "DEFAULT_SEMICOLON"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "string",
        intellij_names: &["DEFAULT_STRING"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "string.escape",
        intellij_names: &["DEFAULT_VALID_STRING_ESCAPE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type",
        intellij_names: &["DEFAULT_CLASS_REFERENCE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type.class",
        intellij_names: &["DEFAULT_CLASS_REFERENCE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type.definition",
        intellij_names: &["DEFAULT_CLASS_REFERENCE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type.enum",
        intellij_names: &["DEFAULT_CLASS_REFERENCE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type.interface",
        intellij_names: &["DEFAULT_CLASS_REFERENCE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type.parameter",
        intellij_names: &["LSP_TYPE_PARAMETER", "TYPE_PARAMETER_NAME_ATTRIBUTES"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "type.struct",
        intellij_names: &["DEFAULT_CLASS_REFERENCE"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "variable",
        intellij_names: &["DEFAULT_IDENTIFIER"],
        allow_inherited: true,
    },
    SyntaxMapping {
        zed_name: "variable.parameter",
        intellij_names: &["DEFAULT_IDENTIFIER"],
        allow_inherited: true,
    },
];

fn apply_theme_colors(scheme: &IntelliJColorScheme, style: &mut ThemeStyleContent) {
    let ui_background = scheme
        .color("LOOKUP_COLOR")
        .or_else(|| scheme.color("DOCUMENTATION_COLOR"))
        .or_else(|| scheme.color("RECENT_LOCATIONS_SELECTION"));
    let editor_background = scheme.color("CONSOLE_BACKGROUND_KEY");
    let active_line_background = scheme.color("CARET_ROW_COLOR");
    let border = scheme.color("RIGHT_MARGIN_COLOR");
    let border_variant = scheme.color("INDENT_GUIDE");
    let active_wrap_guide = scheme.color("SELECTED_INDENT_GUIDE");
    let line_number = scheme.color("LINE_NUMBERS_COLOR");
    let active_line_number = scheme.color("LINE_NUMBER_ON_CARET_ROW_COLOR");
    let scrollbar_thumb = scheme.color("ScrollBar.Mac.thumbColor");
    let scrollbar_thumb_hover = scheme.color("ScrollBar.Mac.hoverThumbColor");
    let text = resolve_foreground(
        scheme,
        &["DEFAULT_IDENTIFIER", "CONSOLE_NORMAL_OUTPUT"],
        true,
    );
    let text_muted = scheme.color("ANNOTATIONS_COLOR");
    let text_placeholder = scheme.color("WHITESPACES");
    let icon = scheme
        .color("ANNOTATIONS_LAST_COMMIT_COLOR")
        .or_else(|| scheme.color("CARET_COLOR"));
    let link = resolve_foreground(scheme, &["CTRL_CLICKABLE"], true);

    set_if_some(&mut style.colors.background, ui_background.clone());
    set_if_some(&mut style.colors.surface_background, ui_background.clone());
    set_if_some(
        &mut style.colors.elevated_surface_background,
        ui_background.clone(),
    );
    set_if_some(
        &mut style.colors.title_bar_background,
        ui_background.clone(),
    );
    set_if_some(&mut style.colors.tab_bar_background, ui_background.clone());
    set_if_some(
        &mut style.colors.tab_inactive_background,
        ui_background.clone(),
    );
    set_if_some(&mut style.colors.panel_background, ui_background.clone());
    set_if_some(
        &mut style.colors.editor_subheader_background,
        ui_background.clone(),
    );
    set_if_some(
        &mut style.colors.editor_background,
        editor_background.clone(),
    );
    set_if_some(
        &mut style.colors.editor_gutter_background,
        editor_background.clone(),
    );
    set_if_some(
        &mut style.colors.terminal_background,
        editor_background.clone(),
    );
    set_if_some(
        &mut style.colors.toolbar_background,
        editor_background.clone(),
    );
    set_if_some(
        &mut style.colors.tab_active_background,
        editor_background.clone(),
    );
    set_if_some(
        &mut style.colors.editor_active_line_background,
        active_line_background.clone(),
    );
    set_if_some(
        &mut style.colors.editor_highlighted_line_background,
        active_line_background,
    );
    set_if_some(&mut style.colors.border, border.clone());
    set_if_some(&mut style.colors.editor_wrap_guide, border);
    set_if_some(&mut style.colors.border_variant, border_variant.clone());
    set_if_some(&mut style.colors.border_disabled, border_variant);
    set_if_some(
        &mut style.colors.editor_active_wrap_guide,
        active_wrap_guide,
    );
    set_if_some(&mut style.colors.editor_line_number, line_number);
    set_if_some(
        &mut style.colors.editor_active_line_number,
        active_line_number,
    );
    set_if_some(
        &mut style.colors.scrollbar_thumb_background,
        scrollbar_thumb,
    );
    set_if_some(
        &mut style.colors.scrollbar_thumb_hover_background,
        scrollbar_thumb_hover,
    );
    set_if_some(&mut style.colors.text, text.clone());
    set_if_some(&mut style.colors.editor_foreground, text.clone());
    set_if_some(&mut style.colors.terminal_foreground, text);
    set_if_some(&mut style.colors.text_muted, text_muted.clone());
    set_if_some(&mut style.colors.icon_muted, text_muted.clone());
    set_if_some(&mut style.colors.icon_placeholder, text_muted);
    set_if_some(&mut style.colors.text_placeholder, text_placeholder.clone());
    set_if_some(&mut style.colors.text_disabled, text_placeholder.clone());
    set_if_some(&mut style.colors.icon, icon.clone());
    set_if_some(&mut style.colors.icon_accent, link.clone());
    set_if_some(&mut style.colors.link_text_hover, link);
}

fn apply_syntax_colors(scheme: &IntelliJColorScheme, style: &mut ThemeStyleContent) {
    for mapping in SYNTAX_MAPPINGS {
        let Some(highlight) =
            resolve_highlight(scheme, mapping.intellij_names, mapping.allow_inherited)
        else {
            continue;
        };

        if highlight.is_empty() {
            continue;
        }

        style
            .syntax
            .entry(mapping.zed_name.to_string())
            .and_modify(|existing| merge_highlight(existing, &highlight))
            .or_insert(highlight);
    }
}

fn resolve_foreground(
    scheme: &IntelliJColorScheme,
    names: &[&str],
    allow_inherited: bool,
) -> Option<String> {
    resolve_highlight(scheme, names, allow_inherited).and_then(|highlight| highlight.color)
}

fn resolve_highlight(
    scheme: &IntelliJColorScheme,
    names: &[&str],
    allow_inherited: bool,
) -> Option<HighlightStyleContent> {
    names.iter().find_map(|name| {
        let (resolved, raw) = scheme.resolved_attribute_with_raw(name)?;
        if !allow_inherited && !raw.has_explicit_style() {
            return None;
        }

        let highlight = highlight_from_attributes(&resolved);
        if highlight.is_empty() {
            None
        } else {
            Some(highlight)
        }
    })
}

fn highlight_from_attributes(attributes: &IntelliJTextAttributes) -> HighlightStyleContent {
    HighlightStyleContent {
        color: attributes.foreground.clone(),
        background_color: attributes.background.clone(),
        font_style: font_style_from_type(attributes.font_type),
        font_weight: font_weight_from_type(attributes.font_type),
    }
}

fn font_style_from_type(font_type: Option<u8>) -> Option<FontStyleContent> {
    match font_type {
        Some(2) | Some(3) => Some(FontStyleContent::Italic),
        _ => None,
    }
}

fn font_weight_from_type(font_type: Option<u8>) -> Option<FontWeightContent> {
    match font_type {
        Some(1) | Some(3) => Some(FontWeightContent::BOLD),
        _ => None,
    }
}

fn merge_highlight(existing: &mut HighlightStyleContent, incoming: &HighlightStyleContent) {
    if incoming.color.is_some() {
        existing.color = incoming.color.clone();
    }
    if incoming.background_color.is_some() {
        existing.background_color = incoming.background_color.clone();
    }
    if incoming.font_style.is_some() {
        existing.font_style = incoming.font_style.clone();
    }
    if incoming.font_weight.is_some() {
        existing.font_weight = incoming.font_weight;
    }
}

fn set_if_some(slot: &mut Option<String>, value: Option<String>) {
    if value.is_some() {
        *slot = value;
    }
}

fn normalize_hex(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with('#') {
        trimmed.to_ascii_lowercase()
    } else {
        format!("#{}", trimmed.to_ascii_lowercase())
    }
}

fn extract_attribute(line: &str, attribute_name: &str) -> Option<String> {
    let marker = format!("{attribute_name}=\"");
    let start = line.find(&marker)? + marker.len();
    let end = line[start..].find('"')? + start;
    Some(line[start..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_intellij_scheme_and_resolves_inheritance() {
        let scheme = IntelliJColorScheme::parse(
            r#"
            <scheme name="Dark" version="142" parent_scheme="Darcula">
              <colors>
                <option name="CONSOLE_BACKGROUND_KEY" value="1E1F22" />
              </colors>
              <attributes>
                <option name="DEFAULT_IDENTIFIER">
                  <value>
                    <option name="FOREGROUND" value="BCBEC4" />
                  </value>
                </option>
                <option name="DEFAULT_FUNCTION_CALL" baseAttributes="DEFAULT_IDENTIFIER" />
                <option name="DEFAULT_KEYWORD">
                  <value>
                    <option name="FOREGROUND" value="CF8E6D" />
                    <option name="FONT_TYPE" value="1" />
                  </value>
                </option>
              </attributes>
            </scheme>
            "#,
        )
        .unwrap();

        assert_eq!(scheme.name, "Dark");
        assert_eq!(
            scheme.color("CONSOLE_BACKGROUND_KEY").as_deref(),
            Some("#1e1f22")
        );

        let (resolved_call, raw_call) = scheme
            .resolved_attribute_with_raw("DEFAULT_FUNCTION_CALL")
            .unwrap();
        assert_eq!(resolved_call.foreground.as_deref(), Some("#bcbec4"));
        assert!(!raw_call.has_explicit_style());

        let keyword = resolve_highlight(&scheme, &["DEFAULT_KEYWORD"], true).unwrap();
        assert_eq!(keyword.color.as_deref(), Some("#cf8e6d"));
        assert_eq!(keyword.font_weight, Some(FontWeightContent::BOLD));
    }

    #[test]
    fn skips_inherited_only_mappings_when_requested() {
        let scheme = IntelliJColorScheme::parse(
            r#"
            <scheme name="Dark" version="142">
              <attributes>
                <option name="DEFAULT_IDENTIFIER">
                  <value>
                    <option name="FOREGROUND" value="BCBEC4" />
                  </value>
                </option>
                <option name="DEFAULT_FUNCTION_CALL" baseAttributes="DEFAULT_IDENTIFIER" />
              </attributes>
            </scheme>
            "#,
        )
        .unwrap();

        assert!(resolve_highlight(&scheme, &["DEFAULT_FUNCTION_CALL"], false).is_none());
        assert_eq!(
            resolve_highlight(&scheme, &["DEFAULT_FUNCTION_CALL"], true)
                .unwrap()
                .color
                .as_deref(),
            Some("#bcbec4")
        );
    }
}
