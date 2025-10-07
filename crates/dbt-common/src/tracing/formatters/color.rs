use console::Style;

pub fn maybe_apply_color(style: &Style, value: &str, colorize: bool) -> String {
    if colorize {
        style.apply_to(value).to_string()
    } else {
        value.to_string()
    }
}
