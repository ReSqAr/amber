use crate::dsl_definition::types::CommandLine;

/// Tokenize a DSL line while respecting single/double quotes and stripping trailing comments.
fn tokenize_line(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut can_be_empty = false;

    for c in line.chars() {
        // Unquoted '#' starts a comment.
        if c == '#' && !in_single_quote && !in_double_quote {
            break;
        }
        if c.is_whitespace() && !in_single_quote && !in_double_quote {
            if !token.is_empty() {
                tokens.push(token.clone());
                token.clear();
            }
        } else if c == '\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
            can_be_empty = true;
        } else if c == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            can_be_empty = true;
        } else {
            token.push(c);
        }
    }
    if !token.is_empty() || can_be_empty {
        tokens.push(token);
    }
    tokens
}

/// Parse a DSL line into one of the refined CommandLine variants.
#[allow(clippy::indexing_slicing)]
pub fn parse_line(line: &str) -> Option<CommandLine> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let tokens = tokenize_line(trimmed);
    if tokens.is_empty() {
        return None;
    }
    match tokens[0].as_str() {
        "assert_equal" => {
            if tokens.len() != 3 {
                panic!("Invalid assert_equal command: {}", line);
            }
            Some(CommandLine::AssertEqual {
                left_repo: tokens[1].to_string(),
                right_repo: tokens[2].to_string(),
            })
        }
        // <-- New DSL command parsing branch added here:
        "assert_output_contains" => {
            if tokens.len() != 2 {
                panic!("Invalid assert_output_contains command: {}", line);
            }
            Some(CommandLine::AssertOutputContains {
                expected: tokens[1].to_string(),
            })
        }
        token if token.starts_with('@') => {
            let repo = token[1..].to_string();
            if tokens.len() < 2 {
                panic!("No command specified for repo {} in line: {}", repo, line);
            }
            let command = tokens[1].as_str();
            match command {
                "amber" => {
                    // All tokens from index 1 onward form the command.
                    let sub_command = tokens[1..].iter().map(|s| s.to_string()).collect();
                    Some(CommandLine::AmberCommand { repo, sub_command })
                }
                "expect" => {
                    // Format is: expect <expected failure> amber <sub cmds>
                    if tokens.len() < 4 {
                        panic!("Invalid expect command: {}", line);
                    }
                    let expected_failure = tokens[2].to_string();
                    let sub_command = tokens[3..].iter().map(|s| s.to_string()).collect();
                    Some(CommandLine::AmberCommandFailure {
                        repo,
                        sub_command,
                        expected_failure,
                    })
                }
                "random_file" => {
                    if tokens.len() != 4 {
                        panic!("Invalid random_file command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    let size: usize = tokens[3]
                        .parse()
                        .expect("Invalid size in random_file command");
                    Some(CommandLine::RandomFile {
                        repo,
                        filename,
                        size,
                    })
                }
                "write_file" => {
                    if tokens.len() != 4 {
                        panic!("Invalid write_file command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    let data = tokens[3].to_string();
                    Some(CommandLine::WriteFile {
                        repo,
                        filename,
                        data,
                    })
                }
                "remove_file" => {
                    if tokens.len() != 3 {
                        panic!("Invalid remove command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    Some(CommandLine::RemoveFile { repo, filename })
                }
                "assert_exists" => {
                    // Can be either: @repo assert_exists filename
                    // or: @repo assert_exists filename "expected content"
                    if tokens.len() < 3 || tokens.len() > 4 {
                        panic!("Invalid assert_exists command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    let content = if tokens.len() == 4 {
                        Some(tokens[3].to_string())
                    } else {
                        None
                    };
                    Some(CommandLine::AssertExists {
                        repo,
                        filename,
                        content,
                    })
                }
                "assert_does_not_exist" => {
                    if tokens.len() != 3 {
                        panic!("Invalid assert_does_not_exist command: {}", line);
                    }
                    let filename = tokens[2].to_string();
                    Some(CommandLine::AssertDoesNotExist { repo, filename })
                }
                "assert_hardlinked" => {
                    if tokens.len() != 4 {
                        panic!("Invalid assert_hardlinked command: {}", line);
                    }
                    let filename1 = tokens[2].to_string();
                    let filename2 = tokens[3].to_string();
                    Some(CommandLine::AssertHardlinked {
                        repo,
                        filename1,
                        filename2,
                    })
                }
                "assert_not_hardlinked" => {
                    if tokens.len() != 4 {
                        panic!("Invalid assert_not_hardlinked command: {}", line);
                    }
                    let filename1 = tokens[2].to_string();
                    let filename2 = tokens[3].to_string();
                    Some(CommandLine::AssertNotHardlinked {
                        repo,
                        filename1,
                        filename2,
                    })
                }
                "start_ssh" => {
                    if tokens.len() != 4 {
                        panic!("Invalid start_ssh command: {}", line);
                    }
                    let port = tokens[2]
                        .parse::<u16>()
                        .expect("Invalid port in start_ssh command");
                    let password = tokens[3].to_string();
                    Some(CommandLine::StartSsh {
                        repo,
                        port,
                        password,
                    })
                }
                "end_ssh" => {
                    if tokens.len() != 2 {
                        panic!("Invalid end_ssh command: {}", line);
                    }
                    Some(CommandLine::EndSsh { repo })
                }
                other => panic!("Unknown repository command '{}' in line: {}", other, line),
            }
        }
        _ => panic!("Unrecognized DSL command: {}", line),
    }
}
