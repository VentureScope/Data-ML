from typing import Dict, List, Optional
from pydantic import BaseModel


class RoleEntry(BaseModel):
    canonical_role: str
    aliases: List[str]


class StandardTechRoles:
    """Company-standard canonical roles and aliases.

    Use `find_canonical(title)` to map an arbitrary job title to a canonical role.
    Matches are case-insensitive and check alias substring containment.
    """

    ROLES: Dict[str, List[str]] = {
        "Software Engineer": ["software engineer", "software developer", "software dev", "developer", "swe", "software eng"],
        "Frontend Developer": ["frontend developer", "front-end developer", "front end developer", "frontend engineer", "react developer", "reactjs developer", "angular developer", "vue developer", "javascript developer", "js developer", "ui developer", "web frontend"],
        "Backend Developer": ["backend developer", "back-end developer", "back end developer", "backend engineer", "server-side developer", "api developer", "node.js developer", "node developer", "django developer", "rails developer", "spring developer"],
        "Full Stack Developer": ["full stack developer", "fullstack developer", "full-stack engineer", "fullstack engineer", "full stack engineer"],
        "Web Developer": ["web developer", "web engineer", "website developer", "web programmer", "frontend/backend web developer"],
        "Mobile Developer": ["mobile developer", "mobile engineer", "mobile app developer", "apps developer"],
        "Android Developer": ["android developer", "android engineer", "kotlin developer", "android app developer"],
        "iOS Developer": ["ios developer", "ios engineer", "swift developer", "objective-c developer", "ios app developer"],
        "Game Developer": ["game developer", "game programmer", "game engineer", "unity developer", "unreal developer", "game dev"],
        "Embedded Systems Engineer": ["embedded engineer", "embedded systems engineer", "firmware engineer", "firmware developer", "embedded developer"],
        "Firmware Engineer": ["firmware engineer", "firmware developer", "embedded firmware engineer"],
        "DevOps Engineer": ["devops engineer", "dev ops engineer", "devops", "ci/cd engineer", "build engineer", "infrastructure engineer"],
        "Site Reliability Engineer": ["site reliability engineer", "sre", "reliability engineer"],
        "Platform Engineer": ["platform engineer", "platform developer", "infra engineer", "infrastructure engineer"],
        "Cloud Engineer": ["cloud engineer", "cloud architect", "aws engineer", "azure engineer", "gcp engineer", "cloud developer"],
        "Systems Administrator": ["system administrator", "sysadmin", "systems admin", "linux administrator", "windows administrator", "systems engineer"],
        "Database Administrator": ["database administrator", "dba", "sql dba", "oracle dba", "mysql dba", "postgres dba"],
        "Data Scientist": ["data scientist", "datascientist", "ml scientist", "machine learning scientist", "research scientist", "statistician"],
        "Data Engineer": ["data engineer", "etl engineer", "big data engineer", "spark engineer", "hadoop engineer", "pipeline engineer"],
        "Machine Learning Engineer": ["machine learning engineer", "ml engineer", "ml developer", "mlops engineer", "model engineer"],
        "MLOps Engineer": ["mlops engineer", "ml ops engineer", "machine learning ops"],
        "AI Engineer": ["ai engineer", "artificial intelligence engineer", "ai developer"],
        "NLP Engineer": ["nlp engineer", "natural language processing engineer", "nlp developer", "nlp scientist"],
        "Computer Vision Engineer": ["computer vision engineer", "cv engineer", "vision engineer", "image processing engineer"],
        "Robotics Engineer": ["robotics engineer", "robotics software engineer", "robotics developer"],
        "Data Analyst": ["data analyst", "analyst", "business data analyst", "sql analyst", "reporting analyst"],
        "BI Analyst": ["bi analyst", "business intelligence analyst", "tableau developer", "power bi developer", "looker developer"],
        "Analytics Engineer": ["analytics engineer", "analytics developer", "dbt developer", "data modeling engineer"],
        "ETL Developer": ["etl developer", "data integration developer", "etl engineer"],
        "QA Engineer": ["qa engineer", "quality assurance engineer", "test engineer", "qa tester", "quality engineer"],
        "QA Automation Engineer": ["qa automation engineer", "automation tester", "test automation engineer", "selenium engineer", "qa automation"],
        "QA Manual": ["manual tester", "qa manual", "qa analyst", "manual tester"],
        "Test Engineer": ["test engineer", "software test engineer", "testing engineer"],
        "Security Engineer": ["security engineer", "application security engineer", "infosec engineer", "security analyst", "secops engineer"],
        "Penetration Tester": ["penetration tester", "pentester", "ethical hacker", "security tester"],
        "DevSecOps Engineer": ["devsecops", "devsecops engineer", "security automation engineer"],
        "Network Engineer": ["network engineer", "network administrator", "network architect", "network specialist"],
        "Systems Engineer": ["systems engineer", "infrastructure engineer", "platform engineer", "ops engineer"],
        "Hardware Engineer": ["hardware engineer", "electrical engineer", "electronics engineer", "pcb designer", "hardware design engineer"],
        "FPGA Engineer": ["fpga engineer", "fpga developer", "hdl developer"],
        "Embedded Software Developer": ["embedded software developer", "embedded software engineer", "firmware developer"],
        "Blockchain Developer": ["blockchain developer", "smart contract developer", "solidity developer", "web3 developer", "blockchain engineer"],
        "Smart Contract Engineer": ["smart contract engineer", "solidity engineer", "solidity developer"],
        "Security Analyst": ["security analyst", "incident responder", "security operations"],
        "Technical Support Engineer": ["technical support engineer", "support engineer", "it support", "helpdesk", "help desk technician"],
        "IT Support": ["it support", "it technician", "desktop support", "helpdesk technician"],
        "Product Manager (Technical)": ["technical product manager", "product manager", "tpm", "product owner"],
        "Technical Program Manager": ["technical program manager", "tpm", "program manager (technical)"],
        "UX Designer": ["ux designer", "user experience designer", "ux researcher", "ux specialist"],
        "UI Designer": ["ui designer", "user interface designer", "visual designer", "interface designer"],
        "UX Researcher": ["ux researcher", "user researcher", "design researcher"],
        "Front-End Framework Specialist": ["react developer", "reactjs developer", "angular developer", "vue developer", "svelte developer"],
        "Java Developer": ["java developer", "java engineer", "spring developer", "java backend"],
        "Python Developer": ["python developer", "python engineer", "django developer", "flask developer", "fastapi developer"],
        "JavaScript Developer": ["javascript developer", "js developer", "node.js developer", "node developer", "express developer"],
        "TypeScript Developer": ["typescript developer", "ts developer", "typescript engineer"],
        "C# Developer": ["c# developer", "csharp developer", ".net developer", "dotnet developer"],
        "C/C++ Developer": ["c developer", "c++ developer", "cpp developer", "embedded c developer"],
        "Go Developer": ["golang developer", "go developer", "go engineer"],
        "Rust Developer": ["rust developer", "rust engineer"],
        "Ruby Developer": ["ruby developer", "ruby on rails developer", "rails developer"],
        "PHP Developer": ["php developer", "php engineer", "laravel developer", "wordpress developer"],
        "Scala Developer": ["scala developer", "scala engineer"],
        "Perl Developer": ["perl developer"],
        "MATLAB Engineer": ["matlab engineer", "mathematical programmer"],
        "Data Warehouse Engineer": ["data warehouse engineer", "dw engineer", "olap developer"],
        "BI Developer": ["bi developer", "report developer", "tableau developer", "power bi developer"],
        "Observability Engineer": ["observability engineer", "monitoring engineer", "telemetry engineer"],
        "Release Engineer": ["release engineer", "build release engineer", "build engineer"],
        "Automation Engineer (General)": ["automation engineer", "automation developer", "rpa developer", "robotic process automation"],
        "Accessibility Engineer": ["accessibility engineer", "a11y engineer", "inclusive design engineer"],
        "Localization Engineer": ["localization engineer", "i18n engineer", "l10n engineer"],
        "Configuration Management Engineer": ["configuration management engineer", "cm engineer", "ansible engineer", "chef puppet engineer"],
        "Others-Technical": ["technical writer", "technical recruiter"],
    }

    @classmethod
    def find_canonical(cls, title: str) -> Optional[str]:
        """Return canonical role for a given job title or None if unknown."""
        if not title:
            return None
        t = title.lower().strip()
        # exact alias match or substring match
        for canon, aliases in cls.ROLES.items():
            for a in aliases:
                if a in t or t in a:
                    return canon
        return None

    @classmethod
    def all_canonical(cls) -> List[str]:
        return list(cls.ROLES.keys())


# Prompt utilities for model calls
SYSTEM_PROMPT = (
    "You are a classification assistant.\n"
    "Task: Given a job posting or job title, determine whether the role is a technical/engineering role (is_tech).\n"
    "If it is technical, map it to the single best canonical role from the provided list.\n"
    "Respond only with a JSON object exactly with these keys: `is_tech` (true/false), `predicted_role` (string or null), and `confidence` (a number between 0.0 and 1.0).\n"
    "Do not include any additional text or explanation.\n"
)


def build_prompt(job_text: str) -> str:
    """Return a full prompt (system + canonical roles + job text) for the model.

    Use this output as the `prompt` value when calling the Gemini endpoint.
    """
    roles_list = ", ".join(StandardTechRoles.all_canonical())
    return (
        SYSTEM_PROMPT
        + "\nCanonical roles: "
        + roles_list
        + "\n\nInput job posting:\n"
        + job_text
    )


__all__ = ["RoleEntry", "StandardTechRoles"]
