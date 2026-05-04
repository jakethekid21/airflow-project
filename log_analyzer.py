import sys
from pathlib import Path


def analyze_file(file_path):
    error_count = 0
    error_messages = []

    with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
        for line in file:
            if "ERROR" in line:
                error_count += 1
                error_messages.append(f"{file_path}: {line.strip()}")

    return error_count, error_messages


def analyze_logs(log_dir):
    total_errors = 0
    all_errors = []

    log_files = Path(log_dir).rglob("*.log")

    for file_path in log_files:
        count, messages = analyze_file(file_path)
        total_errors += count
        all_errors.extend(messages)

    print(f"Total number of errors: {total_errors}")
    print("Here are all the errors:")

    if all_errors:
        for error in all_errors:
            print(error)
    else:
        print("No errors found.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 log_analyzer.py <log_directory>")
        sys.exit(1)

    analyze_logs(sys.argv[1])