# Zed Theme Importer

```sh
cargo run -p theme_importer -- dark-plus-syntax-color-theme.json --output output-theme.json
```

To import an IntelliJ IDEA `.icls` color scheme into an existing Zed theme family:

```sh
cargo run -p theme_importer -- \
  --format intellij \
  ~/Desktop/Dark.icls \
  --base-theme assets/themes/jetbrains/jetbrains.json \
  --output assets/themes/jetbrains/jetbrains.json
```

Or use the helper script that updates the Zed theme file and the installed extension theme:

```sh
./script/import-intellij-theme
```

If you explicitly want to replace `experimental.theme_overrides` in your settings with the
imported result, run:

```sh
ZED_SYNC_THEME_OVERRIDES=1 ./script/import-intellij-theme
```
