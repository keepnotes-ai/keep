"""
Hatch build hook: builds the OpenClaw plugin before packaging.

Runs `npm install && npm run build` in keep/data/openclaw-plugin/
to produce dist/index.js (bundled MCP SDK + plugin code).
"""

import os
import subprocess
import shutil
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class OpenClawPluginBuildHook(BuildHookInterface):
    PLUGIN_NAME = "openclaw-plugin-build"

    def initialize(self, version, build_data):
        plugin_dir = Path(self.root) / "keep" / "data" / "openclaw-plugin"
        dist_file = plugin_dir / "dist" / "index.js"

        # Skip if already built (e.g., running in CI with pre-built artifacts)
        if dist_file.exists():
            self.app.display_info(
                f"OpenClaw plugin already built: {dist_file}"
            )
            return

        # Check for npm/node
        npm = shutil.which("npm")
        if not npm:
            self.app.display_warning(
                "npm not found — skipping OpenClaw plugin build. "
                "The plugin will fall back to legacy CLI mode."
            )
            return

        node = shutil.which("node")
        if not node:
            self.app.display_warning(
                "node not found — skipping OpenClaw plugin build."
            )
            return

        self.app.display_info("Building OpenClaw plugin (npm install + build)...")

        try:
            subprocess.run(
                [npm, "install", "--ignore-scripts"],
                cwd=str(plugin_dir),
                check=True,
                capture_output=True,
                timeout=60,
            )
            subprocess.run(
                [npm, "run", "build"],
                cwd=str(plugin_dir),
                check=True,
                capture_output=True,
                timeout=30,
            )
            self.app.display_info(f"OpenClaw plugin built: {dist_file}")
        except subprocess.CalledProcessError as exc:
            stderr = exc.stderr.decode() if exc.stderr else ""
            self.app.display_warning(
                f"OpenClaw plugin build failed (non-fatal): {stderr[:200]}"
            )
        except FileNotFoundError:
            self.app.display_warning(
                "npm/node not available — skipping OpenClaw plugin build."
            )
        except subprocess.TimeoutExpired:
            self.app.display_warning(
                "OpenClaw plugin build timed out — skipping."
            )
