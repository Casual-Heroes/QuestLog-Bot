"""
Script to rename Warden → QuestLog throughout the codebase
Preserves database names and infrastructure
"""
import os
import re
from pathlib import Path

# Files to process
EXCLUDE_PATTERNS = [
    '__pycache__',
    '.git',
    'wardenprj',
    '.env',
    'log',
    'rename_to_questlog.py',
    'Archive',
]

# Replacements (case-sensitive)
REPLACEMENTS = [
    # User-facing text
    ('Warden Bot', 'QuestLog'),
    ('Warden bot', 'QuestLog'),
    ('warden bot', 'QuestLog'),

    # Commands
    ('/warden', '/questlog'),
    ('name="warden"', 'name="questlog"'),
    ("name='warden'", "name='questlog'"),

    # Descriptions
    ('Warden Bot -', 'QuestLog -'),
    ('warden help', 'questlog help'),
    ('warden dashboard', 'questlog dashboard'),
    ('warden setup', 'questlog setup'),

    # Class names and comments (be careful here)
    ('WardenBot', 'QuestLogBot'),

    # Documentation
    ('The Warden', 'QuestLog'),
]

# DO NOT replace these (database/infrastructure)
PROTECTED_STRINGS = [
    'warden_',  # Database table prefixes
    'DB_NAME=warden',  # Env var
    'DB_USER=warden',  # Env var
    "database': 'warden'",  # Config
    'db.warden',  # Database references
]

def should_skip_file(filepath):
    """Check if file should be skipped"""
    path_str = str(filepath)
    for pattern in EXCLUDE_PATTERNS:
        if pattern in path_str:
            return True
    return False

def is_protected_line(line):
    """Check if line contains protected strings"""
    for protected in PROTECTED_STRINGS:
        if protected in line:
            return True
    return False

def replace_in_file(filepath):
    """Replace Warden with QuestLog in a file"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        original_content = content
        lines = content.split('\n')
        new_lines = []

        for line in lines:
            # Skip protected lines
            if is_protected_line(line):
                new_lines.append(line)
                continue

            # Apply replacements
            new_line = line
            for old, new in REPLACEMENTS:
                new_line = new_line.replace(old, new)

            new_lines.append(new_line)

        new_content = '\n'.join(new_lines)

        # Only write if changed
        if new_content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            return True

        return False

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return False

def main():
    """Main rename process"""
    base_path = Path('/mnt/gamestoreage2/DiscordBots/wardenbot')

    # Find all .py files
    py_files = []
    for root, dirs, files in os.walk(base_path):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in EXCLUDE_PATTERNS]

        for file in files:
            if file.endswith('.py'):
                filepath = Path(root) / file
                if not should_skip_file(filepath):
                    py_files.append(filepath)

    print(f"Found {len(py_files)} Python files to process")

    changed_files = []
    for filepath in py_files:
        if replace_in_file(filepath):
            changed_files.append(filepath)
            print(f"✓ Updated: {filepath.relative_to(base_path)}")

    print(f"\n✅ Complete! Updated {len(changed_files)} files")

    if changed_files:
        print("\nChanged files:")
        for f in changed_files:
            print(f"  - {f.relative_to(base_path)}")

if __name__ == "__main__":
    main()
