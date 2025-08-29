use minijinja::{Error as MinijinjaError, Value};

use crate::AgateTable;

pub fn print_table(
    table: &AgateTable,
    max_rows: usize,
    max_columns: usize,
    max_column_width: usize,
) -> Result<Value, MinijinjaError> {
    // Parse arguments or use defaults matching Python implementation
    // Extract arguments if provided

    // Get character constants (equivalent to Python config options)
    let ellipsis = "...";
    let truncation = "...";
    let h_line = "-";
    let v_line = "|";

    // Get column names and truncate if needed
    let column_names = table.column_names();
    let mut display_column_names = Vec::new();
    for name in column_names.iter().take(max_columns) {
        if name.len() > max_column_width {
            display_column_names.push(format!(
                "{}{}",
                &name[..max_column_width - truncation.len()],
                truncation
            ));
        } else {
            display_column_names.push(name.clone());
        }
    }

    let columns_truncated = max_columns < column_names.len();
    if columns_truncated {
        display_column_names.push(ellipsis.to_string());
    }

    // Calculate initial column widths based on headers
    let mut widths = display_column_names
        .iter()
        .map(|name| name.len())
        .collect::<Vec<usize>>();

    // Format the data
    let mut formatted_data = Vec::new();
    let num_rows = table.num_rows();
    let rows_truncated = max_rows < num_rows;

    for i in 0..std::cmp::min(max_rows, num_rows) {
        let mut formatted_row = Vec::new();

        for j in 0..std::cmp::min(max_columns, table.num_columns()) {
            if let Some(cell) = table.cell(i as isize, j as isize) {
                let value = if cell.is_undefined() || cell.is_none() {
                    "".to_string()
                } else {
                    let str_val = cell.to_string();
                    if str_val.len() > max_column_width {
                        format!(
                            "{}{}",
                            &str_val[..max_column_width - truncation.len()],
                            truncation
                        )
                    } else {
                        str_val
                    }
                };

                // Update column width if necessary
                if j < widths.len() && value.len() > widths[j] {
                    widths[j] = value.len();
                }

                formatted_row.push(value);
            } else {
                formatted_row.push("".to_string());
            }
        }

        if columns_truncated {
            formatted_row.push(ellipsis.to_string());
        }

        formatted_data.push(formatted_row);
    }

    // Build the table string
    let mut output = String::new();

    // Helper function to write a row
    let write_row = |output: &mut String, row: &[String], is_header: bool| {
        output.push_str(v_line);
        for (j, value) in row.iter().enumerate() {
            if j < widths.len() {
                // Determine if it's a number or text for alignment
                // Here we're simplifying by just checking if it parses as a number
                let is_number =
                    value.parse::<f64>().is_ok() && !value.is_empty() && value != ellipsis;

                if is_number || is_header {
                    // Right justify numbers and headers
                    output.push_str(&format!(" {} ", value.to_string().pad_left(widths[j])));
                } else {
                    // Left justify text
                    output.push_str(&format!(" {} ", value.to_string().pad_right(widths[j])));
                }
            }

            if j < row.len() - 1 {
                output.push_str(v_line);
            }
        }
        output.push_str(v_line);
        output.push('\n');
    };

    // Write header row
    write_row(&mut output, &display_column_names, true);

    // Write divider
    output.push_str(v_line);
    for (j, &width) in widths.iter().enumerate() {
        output.push_str(&format!(" {} ", h_line.repeat(width)));

        if j < widths.len() - 1 {
            output.push_str(v_line);
        }
    }
    output.push_str(v_line);
    output.push('\n');

    // Write data rows
    for row in formatted_data {
        write_row(&mut output, &row, false);
    }

    // Add truncation row if rows were truncated
    if rows_truncated {
        let ellipsis_row = vec![ellipsis.to_string(); display_column_names.len()];
        write_row(&mut output, &ellipsis_row, false);
    }

    Ok(Value::from(output))
}

// Add helper methods for string padding - used in print_table
trait StringPadding {
    fn pad_right(&self, width: usize) -> String;
    fn pad_left(&self, width: usize) -> String;
}

impl StringPadding for String {
    fn pad_right(&self, width: usize) -> String {
        if self.len() >= width {
            self.clone()
        } else {
            let mut padded = self.clone();
            padded.push_str(&" ".repeat(width - self.len()));
            padded
        }
    }

    fn pad_left(&self, width: usize) -> String {
        if self.len() >= width {
            self.clone()
        } else {
            let mut padded = " ".repeat(width - self.len());
            padded.push_str(self);
            padded
        }
    }
}

impl StringPadding for str {
    fn pad_right(&self, width: usize) -> String {
        self.to_string().pad_right(width)
    }

    fn pad_left(&self, width: usize) -> String {
        self.to_string().pad_left(width)
    }
}
