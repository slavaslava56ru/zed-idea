mod color;
mod intellij;
mod vscode;

use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Context as _, Result};
use clap::{Parser, ValueEnum};
use collections::IndexMap;
use log::LevelFilter;
use serde::Deserialize;
use simplelog::ColorChoice;
use simplelog::{TermLogger, TerminalMode};
use theme::{Appearance, AppearanceContent};
use theme_settings::{ThemeFamilyContent, ThemeStyleContent};

use crate::intellij::IntelliJColorScheme;
use crate::vscode::VsCodeTheme;
use crate::vscode::VsCodeThemeConverter;

const ZED_THEME_SCHEMA_URL: &str = "https://zed.dev/schema/themes/v0.2.0.json";

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThemeAppearanceJson {
    Light,
    Dark,
}

impl From<ThemeAppearanceJson> for AppearanceContent {
    fn from(value: ThemeAppearanceJson) -> Self {
        match value {
            ThemeAppearanceJson::Light => Self::Light,
            ThemeAppearanceJson::Dark => Self::Dark,
        }
    }
}

impl From<ThemeAppearanceJson> for Appearance {
    fn from(value: ThemeAppearanceJson) -> Self {
        match value {
            ThemeAppearanceJson::Light => Self::Light,
            ThemeAppearanceJson::Dark => Self::Dark,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ThemeMetadata {
    pub name: String,
    pub file_name: String,
    pub appearance: ThemeAppearanceJson,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The path to the theme to import.
    theme_path: PathBuf,

    /// The source theme format.
    #[arg(long, value_enum, default_value_t = ThemeFormat::Auto)]
    format: ThemeFormat,

    /// Whether to warn when values are missing from the theme.
    #[arg(long)]
    warn_on_missing: bool,

    /// A Zed theme family file to merge imported values into.
    #[arg(long)]
    base_theme: Option<PathBuf>,

    /// The path to write the output to.
    #[arg(long, short)]
    output: Option<PathBuf>,

    /// The Zed settings file to update with experimental.theme_overrides.
    #[arg(long)]
    settings: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
enum ThemeFormat {
    Auto,
    Vscode,
    Intellij,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let log_config = {
        let mut config = simplelog::ConfigBuilder::new();

        if !args.warn_on_missing {
            config.add_filter_ignore_str("theme_printer");
        }

        config.build()
    };

    TermLogger::init(
        LevelFilter::Trace,
        log_config,
        TerminalMode::Stderr,
        ColorChoice::Auto,
    )
    .expect("could not initialize logger");

    let theme_file_path = args.theme_path;

    let mut buffer = Vec::new();
    match File::open(&theme_file_path).and_then(|mut file| file.read_to_end(&mut buffer)) {
        Ok(_) => {}
        Err(err) => {
            log::info!("Failed to open file at path: {:?}", theme_file_path);
            return Err(err)?;
        }
    };

    match detect_format(args.format, &theme_file_path) {
        ThemeFormat::Vscode => {
            let vscode_theme: VsCodeTheme = serde_json_lenient::from_slice(&buffer)
                .context(format!("failed to parse theme {theme_file_path:?}"))?;

            let theme_metadata = ThemeMetadata {
                name: vscode_theme.name.clone().unwrap_or("".to_string()),
                appearance: ThemeAppearanceJson::Dark,
                file_name: "".to_string(),
            };

            let converter =
                VsCodeThemeConverter::new(vscode_theme, theme_metadata, IndexMap::default());

            let theme = converter.convert()?;
            let mut theme = serde_json::to_value(theme).unwrap();
            theme.as_object_mut().unwrap().insert(
                "$schema".to_string(),
                serde_json::Value::String(ZED_THEME_SCHEMA_URL.to_string()),
            );
            let theme_json = serde_json::to_string_pretty(&theme).unwrap();

            if let Some(output) = args.output {
                let mut file = File::create(output)?;
                file.write_all(theme_json.as_bytes())?;
            } else {
                println!("{}", theme_json);
            }
        }
        ThemeFormat::Intellij => {
            let content = std::str::from_utf8(&buffer).context(format!(
                "failed to decode IntelliJ theme {theme_file_path:?} as utf-8"
            ))?;
            let scheme = IntelliJColorScheme::parse(content).context(format!(
                "failed to parse IntelliJ scheme {theme_file_path:?}"
            ))?;
            let (family, style) = if let Some(base_theme_path) = &args.base_theme {
                let base_theme = load_theme_family(base_theme_path)?;
                scheme.apply_to_theme_family(base_theme)?
            } else {
                scheme.into_theme_family()?
            };

            if let Some(settings_path) = &args.settings {
                update_settings(settings_path, &style)?;
            }

            let mut theme = serde_json::to_value(family).unwrap();
            theme.as_object_mut().unwrap().insert(
                "$schema".to_string(),
                serde_json::Value::String(ZED_THEME_SCHEMA_URL.to_string()),
            );
            let theme_json = serde_json::to_string_pretty(&theme).unwrap();

            if let Some(output) = args.output {
                let mut file = File::create(output)?;
                file.write_all(theme_json.as_bytes())?;
            } else {
                println!("{}", theme_json);
            }
        }
        ThemeFormat::Auto => unreachable!("auto format should be resolved before dispatch"),
    }

    log::info!("Done!");

    Ok(())
}

fn detect_format(requested: ThemeFormat, theme_file_path: &PathBuf) -> ThemeFormat {
    match requested {
        ThemeFormat::Auto => match theme_file_path.extension().and_then(|ext| ext.to_str()) {
            Some("icls") | Some("xml") => ThemeFormat::Intellij,
            _ => ThemeFormat::Vscode,
        },
        explicit => explicit,
    }
}

fn load_theme_family(path: &PathBuf) -> Result<ThemeFamilyContent> {
    let mut buffer = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut buffer))
        .with_context(|| format!("failed to read base theme {path:?}"))?;

    let mut value: serde_json::Value = serde_json_lenient::from_slice(&buffer)
        .with_context(|| format!("failed to parse {path:?}"))?;
    if let Some(object) = value.as_object_mut() {
        object.remove("$schema");
    }

    serde_json::from_value(value).with_context(|| format!("failed to decode base theme {path:?}"))
}

fn update_settings(path: &PathBuf, style: &ThemeStyleContent) -> Result<()> {
    let mut buffer = Vec::new();
    File::open(path)
        .and_then(|mut file| file.read_to_end(&mut buffer))
        .with_context(|| format!("failed to read settings {path:?}"))?;

    let mut settings: serde_json::Value = serde_json_lenient::from_slice(&buffer)
        .with_context(|| format!("failed to parse {path:?}"))?;
    let object = settings
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!("settings root must be a JSON object"))?;
    object.insert(
        "experimental.theme_overrides".to_string(),
        serde_json::to_value(style)?,
    );

    let settings_json = serde_json::to_string_pretty(&settings)?;
    let mut file = File::create(path)?;
    file.write_all(settings_json.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}
