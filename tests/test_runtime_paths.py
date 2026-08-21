import os
import unittest
from unittest.mock import patch

import runtime_paths


class RuntimePathsTests(unittest.TestCase):
    def test_defaults_to_project_directory_when_data_dir_is_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            paths = runtime_paths.get_runtime_paths()

        self.assertEqual(paths.data_dir, runtime_paths.PROJECT_DIR)
        self.assertEqual(paths.env_file, os.path.join(runtime_paths.PROJECT_DIR, ".env"))
        self.assertEqual(paths.state_file, os.path.join(runtime_paths.PROJECT_DIR, "state.json"))
        self.assertEqual(paths.log_dir, os.path.join(runtime_paths.PROJECT_DIR, "logs"))

    def test_uses_data_dir_for_runtime_files_when_configured(self):
        with patch.dict(os.environ, {"DATA_DIR": "/data"}, clear=True):
            paths = runtime_paths.get_runtime_paths()

        self.assertEqual(paths.data_dir, "/data")
        self.assertEqual(paths.env_file, "/data/.env")
        self.assertEqual(paths.state_file, "/data/state.json")
        self.assertEqual(paths.state_backup_dir, "/data/state_backups")
        self.assertEqual(paths.log_dir, "/data/logs")


if __name__ == "__main__":
    unittest.main()
