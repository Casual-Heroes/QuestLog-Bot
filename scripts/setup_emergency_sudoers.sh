#!/bin/bash
# Run this ONCE as root to grant www-data the minimum sudo permissions
# needed for the emergency kill switch cog.
#
# Usage: sudo bash scripts/setup_emergency_sudoers.sh

set -e

SUDOERS_FILE="/etc/sudoers.d/wardenbot-emergency"

cat > "$SUDOERS_FILE" << 'EOF'
# Wardenbot emergency kill switch — www-data can only stop/start these two services
www-data ALL=(ALL) NOPASSWD: /bin/systemctl stop casualheroes
www-data ALL=(ALL) NOPASSWD: /bin/systemctl start casualheroes
www-data ALL=(ALL) NOPASSWD: /bin/systemctl stop matrix-synapse
www-data ALL=(ALL) NOPASSWD: /bin/systemctl start matrix-synapse
EOF

# Lock down the file — sudoers.d files must be 440
chmod 440 "$SUDOERS_FILE"

# Validate syntax before anything can break sudo
visudo -c -f "$SUDOERS_FILE" && echo "✅ Sudoers file installed at $SUDOERS_FILE" || {
    echo "❌ Syntax error — removing broken file"
    rm -f "$SUDOERS_FILE"
    exit 1
}
