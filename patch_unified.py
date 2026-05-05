import re

with open("Job_pipeline/preprocessing/unified_preprocessor.py", "r") as f:
    content = f.read()

# Add import
if "from Job_pipeline.preprocessing.gemini_client import RobustGeminiClient" not in content:
    content = content.replace(
        "from Job_pipeline.preprocessing.remote_detection import RemoteDetectionModule",
        "from Job_pipeline.preprocessing.remote_detection import RemoteDetectionModule\nfrom Job_pipeline.preprocessing.gemini_client import RobustGeminiClient"
    )

# Change gemini_callable initialization
old_init = """        gemini_callable = None
        if not self.config.enable_gemini_fallback:
            gemini_callable = lambda _prompt: None"""

new_init = """        gemini_callable = None
        if self.config.enable_gemini_fallback:
            client = RobustGeminiClient()
            gemini_callable = client
        else:
            gemini_callable = lambda _prompt: None"""

content = content.replace(old_init, new_init)

with open("Job_pipeline/preprocessing/unified_preprocessor.py", "w") as f:
    f.write(content)
