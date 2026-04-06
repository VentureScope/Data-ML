from __future__ import annotations

import unittest

from Job_pipeline.preprocessing.clean_text import CleanTextModule
from Job_pipeline.preprocessing.date_features import DateFeaturesModule
from Job_pipeline.preprocessing.description_embedding import DescriptionEmbeddingModule
from Job_pipeline.preprocessing.education_extraction import EducationExtractionModule
from Job_pipeline.preprocessing.job_id import JobIdConfig, JobIdModule
from Job_pipeline.preprocessing.job_type_extraction import JobTypeExtractionModule
from Job_pipeline.preprocessing.location_extraction import LocationExtractionModule
from Job_pipeline.preprocessing.remote_detection import RemoteDetectionModule
from Job_pipeline.preprocessing.skills_extraction import SkillsExtractionModule
from Job_pipeline.preprocessing.title_normalization import TitleNormalizationModule
from Job_pipeline.tests.test_utils import load_sample_row


class TestPipelineTargetFeatures(unittest.TestCase):
    def test_target_feature_contract(self) -> None:
        row = load_sample_row()

        cleaner = CleanTextModule()
        clean = cleaner.transform(row.get("title"), row.get("description"))

        date_out = DateFeaturesModule().transform(row)

        id_out = JobIdModule(
            JobIdConfig(
                title_key="title",
                company_key="entity_name",
                date_key="created_at",
                source_key="source",
            )
        ).transform(row)

        title_out = TitleNormalizationModule().normalize(clean["clean_title"], clean["clean_description"])
        emb_out = DescriptionEmbeddingModule().transform({"clean_description": clean["clean_description"]})

        location_value = f"{row.get('city', '')}, {row.get('country', '')}".strip(", ")
        loc_out = LocationExtractionModule().extract(clean["clean_description"], location_value=location_value)

        remote_out = RemoteDetectionModule().detect(clean["clean_title"], clean["clean_description"])
        type_out = JobTypeExtractionModule().extract(clean["clean_title"], clean["clean_description"])
        edu_out = EducationExtractionModule().extract(clean["clean_title"], clean["clean_description"])
        skills_out = SkillsExtractionModule().extract(clean["clean_description"])

        result = {
            "year_month": date_out["year_month"],
            "timestamp": date_out["timestamp"],
            "month": date_out["month"],
            "holiday_flag": date_out["holiday_flag"],
            "job_id": id_out["job_id"],
            "job_title": row["title"],
            "normalized_title": title_out["normalized_title"],
            "DescriptionVec": emb_out["DescriptionVec"],
            "city": loc_out["city"],
            "region": loc_out["region"],
            "country": loc_out["country"],
            "is_remote": remote_out["is_remote"],
            "job_type": type_out["job_type"],
            "education_level": edu_out["education_level"],
            "skills": skills_out["skills"],
        }

        required = {
            "year_month",
            "timestamp",
            "month",
            "holiday_flag",
            "job_id",
            "job_title",
            "normalized_title",
            "DescriptionVec",
            "city",
            "region",
            "country",
            "is_remote",
            "job_type",
            "education_level",
            "skills",
        }
        self.assertEqual(set(result.keys()), required)
        self.assertIsInstance(result["DescriptionVec"], list)
        self.assertIsInstance(result["skills"], list)
        self.assertIsInstance(result["is_remote"], bool)


if __name__ == "__main__":
    unittest.main()
