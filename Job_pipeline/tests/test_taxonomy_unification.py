from __future__ import annotations

import json
import unittest
from pathlib import Path


class TestTaxonomyUnification(unittest.TestCase):
    def test_skills_use_multi_role_categories(self) -> None:
        roles_path = Path("Job_pipeline/taxonomy/roles.json")
        skills_path = Path("Job_pipeline/taxonomy/skills.json")

        roles = json.loads(roles_path.read_text(encoding="utf-8"))
        skills = json.loads(skills_path.read_text(encoding="utf-8"))

        canonical_roles = {
            str(item.get("role_name", "")).strip()
            for item in roles
            if str(item.get("role_name", "")).strip()
        }

        self.assertTrue(canonical_roles)
        self.assertGreater(len(skills), 0)

        for skill in skills:
            self.assertNotIn("category", skill)
            self.assertIn("categories", skill)
            self.assertIsInstance(skill["categories"], list)
            self.assertGreater(len(skill["categories"]), 0)
            for category in skill["categories"]:
                self.assertIn(category, canonical_roles)


if __name__ == "__main__":
    unittest.main()
