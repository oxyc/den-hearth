//! Prometheus text exposition, written by hand: the format is a few dozen lines of printing, and a client library
//! would be a dependency for it. Every addon's `/metrics` is built from these.

use std::fmt::{Display, Write};

/// One HELP/TYPE block and its samples: `(labels, value)` pairs, where `labels` is the inside of the braces
/// (`kind="upstream"`) and an empty one is an unlabelled series.
pub fn family<V: Display>(out: &mut String, name: &str, kind: &str, help: &str, samples: &[(&str, V)]) {
    // Writing to a String cannot fail.
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
    for (labels, value) in samples {
        sample(out, name, labels, value);
    }
}

/// One HELP/TYPE block and its single sample.
pub fn series(out: &mut String, name: &str, kind: &str, help: &str, labels: &str, value: impl Display) {
    family(out, name, kind, help, &[(labels, value)]);
}

/// One sample line, without a HELP/TYPE block, for a family that is written a row at a time.
pub fn sample(out: &mut String, name: &str, labels: &str, value: impl Display) {
    if labels.is_empty() {
        let _ = writeln!(out, "{name} {value}");
    } else {
        let _ = writeln!(out, "{name}{{{labels}}} {value}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_family_is_help_type_and_its_samples() {
        let mut out = String::new();
        series(&mut out, "x_build_info", "gauge", "The build.", "version=\"1\"", 1);
        series(&mut out, "x_up", "gauge", "Up.", "", 7u64);
        family(&mut out, "x_fails", "counter", "Fails.", &[("kind=\"a\"", 2u64), ("kind=\"b\"", 3)]);
        assert_eq!(
            out,
            "# HELP x_build_info The build.\n# TYPE x_build_info gauge\nx_build_info{version=\"1\"} 1\n\
             # HELP x_up Up.\n# TYPE x_up gauge\nx_up 7\n\
             # HELP x_fails Fails.\n# TYPE x_fails counter\nx_fails{kind=\"a\"} 2\nx_fails{kind=\"b\"} 3\n"
        );
    }
}
