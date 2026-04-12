"""Tests for coclab.config — layered storage root configuration.

Precedence (highest to lowest):
1. CLI keyword arguments
2. Environment variables
3. Repo-local coclab.yaml
4. User config ~/.config/coclab/config.yaml
5. Built-in defaults
"""

from __future__ import annotations

from pathlib import Path

import pytest

from coclab.config import (
    ENV_ASSET_STORE_ROOT,
    ENV_OUTPUT_ROOT,
    REPO_CONFIG_FILENAME,
    _load_yaml_file,
    load_config,
)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


class TestDefaults:
    """Built-in defaults use repo-relative storage roots."""

    def test_default_asset_store_root(self, tmp_path: Path) -> None:
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == tmp_path / "data"

    def test_default_output_root(self, tmp_path: Path) -> None:
        cfg = load_config(project_root=tmp_path)
        assert cfg.output_root == tmp_path / "outputs"

    def test_frozen(self, tmp_path: Path) -> None:
        cfg = load_config(project_root=tmp_path)
        with pytest.raises(AttributeError):
            cfg.asset_store_root = tmp_path / "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# CLI flags (layer 1 — highest precedence)
# ---------------------------------------------------------------------------


class TestCLIFlags:
    """Explicit keyword arguments override everything."""

    def test_cli_asset_store_root(self, tmp_path: Path) -> None:
        custom = tmp_path / "my-assets"
        cfg = load_config(asset_store_root=custom, project_root=tmp_path)
        assert cfg.asset_store_root == custom

    def test_cli_output_root(self, tmp_path: Path) -> None:
        custom = tmp_path / "my-outputs"
        cfg = load_config(output_root=custom, project_root=tmp_path)
        assert cfg.output_root == custom

    def test_cli_as_string(self, tmp_path: Path) -> None:
        cfg = load_config(asset_store_root="/tmp/assets", project_root=tmp_path)
        assert cfg.asset_store_root == Path("/tmp/assets")

    def test_cli_relative_path_resolves_from_current_working_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime_dir = tmp_path / "runtime"
        runtime_dir.mkdir()
        monkeypatch.chdir(runtime_dir)

        cfg = load_config(output_root="outputs/panel", project_root=tmp_path)

        assert cfg.output_root == (runtime_dir / "outputs" / "panel").resolve()

    def test_cli_overrides_env(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(ENV_ASSET_STORE_ROOT, "/env/assets")
        cfg = load_config(asset_store_root="/cli/assets", project_root=tmp_path)
        assert cfg.asset_store_root == Path("/cli/assets")

    def test_cli_overrides_repo_yaml(self, tmp_path: Path) -> None:
        (tmp_path / REPO_CONFIG_FILENAME).write_text("asset_store_root: /repo/assets\n")
        cfg = load_config(asset_store_root="/cli/assets", project_root=tmp_path)
        assert cfg.asset_store_root == Path("/cli/assets")


# ---------------------------------------------------------------------------
# Environment variables (layer 2)
# ---------------------------------------------------------------------------


class TestEnvVars:
    """Environment variables override config files but not CLI."""

    def test_env_asset_store_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENV_ASSET_STORE_ROOT, "/env/assets")
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path("/env/assets")

    def test_env_output_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv(ENV_OUTPUT_ROOT, "/env/outputs")
        cfg = load_config(project_root=tmp_path)
        assert cfg.output_root == Path("/env/outputs")

    def test_env_overrides_repo_yaml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / REPO_CONFIG_FILENAME).write_text("asset_store_root: /repo/assets\n")
        monkeypatch.setenv(ENV_ASSET_STORE_ROOT, "/env/assets")
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path("/env/assets")

    def test_env_relative_path_resolves_from_current_working_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        runtime_dir = tmp_path / "runtime"
        runtime_dir.mkdir()
        monkeypatch.chdir(runtime_dir)
        monkeypatch.setenv(ENV_OUTPUT_ROOT, "exports/panel")

        cfg = load_config(project_root=tmp_path)

        assert cfg.output_root == (runtime_dir / "exports" / "panel").resolve()


# ---------------------------------------------------------------------------
# Repo-local config (layer 3)
# ---------------------------------------------------------------------------


class TestRepoConfig:
    """Repo-local coclab.yaml overrides user config and defaults."""

    def test_repo_asset_store_root(self, tmp_path: Path) -> None:
        (tmp_path / REPO_CONFIG_FILENAME).write_text("asset_store_root: /repo/assets\n")
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path("/repo/assets")

    def test_repo_output_root(self, tmp_path: Path) -> None:
        (tmp_path / REPO_CONFIG_FILENAME).write_text("output_root: /repo/outputs\n")
        cfg = load_config(project_root=tmp_path)
        assert cfg.output_root == Path("/repo/outputs")

    def test_repo_partial_config(self, tmp_path: Path) -> None:
        """Only the specified key is overridden; others use defaults."""
        (tmp_path / REPO_CONFIG_FILENAME).write_text("asset_store_root: /repo/assets\n")
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path("/repo/assets")
        assert cfg.output_root == tmp_path / "outputs"

    def test_repo_relative_path_resolves_from_project_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        off_repo_dir = tmp_path / "elsewhere"
        off_repo_dir.mkdir()
        monkeypatch.chdir(off_repo_dir)
        (tmp_path / REPO_CONFIG_FILENAME).write_text("output_root: ../HHP-Data\n")

        cfg = load_config(project_root=tmp_path)

        assert cfg.output_root == (tmp_path.parent / "HHP-Data").resolve()

    def test_repo_overrides_user_config(self, tmp_path: Path) -> None:
        user_dir = tmp_path / "user_config"
        user_dir.mkdir()
        user_cfg_path = user_dir / "config.yaml"
        user_cfg_path.write_text("asset_store_root: /user/assets\n")

        (tmp_path / REPO_CONFIG_FILENAME).write_text("asset_store_root: /repo/assets\n")

        import coclab.config as config_mod

        original = config_mod.USER_CONFIG_PATH
        config_mod.USER_CONFIG_PATH = user_cfg_path
        try:
            cfg = load_config(project_root=tmp_path)
            assert cfg.asset_store_root == Path("/repo/assets")
        finally:
            config_mod.USER_CONFIG_PATH = original


# ---------------------------------------------------------------------------
# User config (layer 4)
# ---------------------------------------------------------------------------


class TestUserConfig:
    """User config (~/.config/coclab/config.yaml) overrides only defaults."""

    def test_user_config(self, tmp_path: Path) -> None:
        user_dir = tmp_path / "user_config"
        user_dir.mkdir()
        user_cfg_path = user_dir / "config.yaml"
        user_cfg_path.write_text("output_root: /user/outputs\n")

        import coclab.config as config_mod

        original = config_mod.USER_CONFIG_PATH
        config_mod.USER_CONFIG_PATH = user_cfg_path
        try:
            cfg = load_config(project_root=tmp_path)
            assert cfg.output_root == Path("/user/outputs")
        finally:
            config_mod.USER_CONFIG_PATH = original

    def test_user_relative_path_resolves_from_user_config_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        user_dir = tmp_path / "user_config"
        user_dir.mkdir()
        user_cfg_path = user_dir / "config.yaml"
        user_cfg_path.write_text("output_root: ../shared/panel\n")

        off_repo_dir = tmp_path / "runtime"
        off_repo_dir.mkdir()
        monkeypatch.chdir(off_repo_dir)

        import coclab.config as config_mod

        original = config_mod.USER_CONFIG_PATH
        config_mod.USER_CONFIG_PATH = user_cfg_path
        try:
            cfg = load_config(project_root=tmp_path)
            assert cfg.output_root == (user_dir.parent / "shared" / "panel").resolve()
        finally:
            config_mod.USER_CONFIG_PATH = original


# ---------------------------------------------------------------------------
# YAML loading edge cases
# ---------------------------------------------------------------------------


class TestYAMLLoading:
    """_load_yaml_file handles malformed and missing files gracefully."""

    def test_missing_file(self, tmp_path: Path) -> None:
        assert _load_yaml_file(tmp_path / "nonexistent.yaml") == {}

    def test_empty_file(self, tmp_path: Path) -> None:
        p = tmp_path / "empty.yaml"
        p.write_text("")
        assert _load_yaml_file(p) == {}

    def test_non_dict_content(self, tmp_path: Path) -> None:
        p = tmp_path / "list.yaml"
        p.write_text("- item1\n- item2\n")
        assert _load_yaml_file(p) == {}

    def test_valid_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "valid.yaml"
        p.write_text("asset_store_root: /some/path\noutput_root: /other/path\n")
        result = _load_yaml_file(p)
        assert result == {"asset_store_root": "/some/path", "output_root": "/other/path"}

    def test_invalid_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text(":\n  :\n    - ][")
        assert _load_yaml_file(p) == {}


# ---------------------------------------------------------------------------
# Full precedence integration
# ---------------------------------------------------------------------------


class TestFullPrecedence:
    """Multiple layers active at once resolve correctly."""

    def test_all_layers_cli_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Set up all layers
        monkeypatch.setenv(ENV_ASSET_STORE_ROOT, "/env/assets")
        (tmp_path / REPO_CONFIG_FILENAME).write_text("asset_store_root: /repo/assets\n")

        cfg = load_config(asset_store_root="/cli/assets", project_root=tmp_path)
        assert cfg.asset_store_root == Path("/cli/assets")

    def test_mixed_sources(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """asset_store_root from env, output_root from repo yaml."""
        monkeypatch.setenv(ENV_ASSET_STORE_ROOT, "/env/assets")
        (tmp_path / REPO_CONFIG_FILENAME).write_text("output_root: /repo/outputs\n")

        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == Path("/env/assets")
        assert cfg.output_root == Path("/repo/outputs")

    def test_no_config_anywhere(self, tmp_path: Path) -> None:
        """With no config files or env vars, defaults apply."""
        cfg = load_config(project_root=tmp_path)
        assert cfg.asset_store_root == tmp_path / "data"
        assert cfg.output_root == tmp_path / "outputs"
