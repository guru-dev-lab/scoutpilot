"""
Server-side skill/tech extraction from job titles and descriptions.
Mirrors the client-side SKILL_DB patterns so tags are consistent.
"""
import re

# Each entry: (display_name, regex_pattern, category)
SKILL_DB = [
    # Languages
    ("Python", re.compile(r"\bPython\b", re.I), "lang"),
    ("JavaScript", re.compile(r"\bJavaScript\b", re.I), "lang"),
    ("TypeScript", re.compile(r"\bTypeScript\b", re.I), "lang"),
    ("Java", re.compile(r"\bJava\b(?!\s*Script)", re.I), "lang"),
    ("C#", re.compile(r"\bC#|\.NET\b", re.I), "lang"),
    ("C++", re.compile(r"\bC\+\+\b", re.I), "lang"),
    ("Go", re.compile(r"\bGolang\b|\bGo\b(?=\s+(developer|engineer|programming|language))", re.I), "lang"),
    ("Rust", re.compile(r"\bRust\b", re.I), "lang"),
    ("Ruby", re.compile(r"\bRuby\b", re.I), "lang"),
    ("PHP", re.compile(r"\bPHP\b", re.I), "lang"),
    ("Swift", re.compile(r"\bSwift\b", re.I), "lang"),
    ("Kotlin", re.compile(r"\bKotlin\b", re.I), "lang"),
    ("Scala", re.compile(r"\bScala\b", re.I), "lang"),
    ("R", re.compile(r"\bR\b(?=\s+(programming|studio|language|statistical))", re.I), "lang"),
    ("SQL", re.compile(r"\bSQL\b", re.I), "data"),
    # Data & Analytics
    ("Power BI", re.compile(r"\bPower\s*BI\b", re.I), "data"),
    ("Tableau", re.compile(r"\bTableau\b", re.I), "data"),
    ("Excel", re.compile(r"\bExcel\b", re.I), "data"),
    ("Snowflake", re.compile(r"\bSnowflake\b", re.I), "data"),
    ("BigQuery", re.compile(r"\bBigQuery\b", re.I), "data"),
    ("Databricks", re.compile(r"\bDatabricks\b", re.I), "data"),
    ("Spark", re.compile(r"\bApache\s*Spark\b|\bPySpark\b|\bSpark\b", re.I), "data"),
    ("Hadoop", re.compile(r"\bHadoop\b", re.I), "data"),
    ("Kafka", re.compile(r"\bKafka\b", re.I), "data"),
    ("Airflow", re.compile(r"\bAirflow\b", re.I), "data"),
    ("dbt", re.compile(r"\bdbt\b", re.I), "data"),
    ("Looker", re.compile(r"\bLooker\b", re.I), "data"),
    ("Redshift", re.compile(r"\bRedshift\b", re.I), "data"),
    ("PostgreSQL", re.compile(r"\bPostgres(?:QL)?\b", re.I), "data"),
    ("MongoDB", re.compile(r"\bMongoDB?\b", re.I), "data"),
    ("Redis", re.compile(r"\bRedis\b", re.I), "data"),
    ("Elasticsearch", re.compile(r"\bElasticsearch\b|\bElastic\b", re.I), "data"),
    ("MySQL", re.compile(r"\bMySQL\b", re.I), "data"),
    ("Oracle DB", re.compile(r"\bOracle\s*(DB|Database)\b", re.I), "data"),
    ("ETL", re.compile(r"\bETL\b", re.I), "data"),
    # Cloud
    ("AWS", re.compile(r"\bAWS\b|\bAmazon Web Services\b", re.I), "cloud"),
    ("Azure", re.compile(r"\bAzure\b", re.I), "cloud"),
    ("GCP", re.compile(r"\bGCP\b|\bGoogle Cloud\b", re.I), "cloud"),
    ("Salesforce", re.compile(r"\bSalesforce\b|\bSFDC\b", re.I), "cloud"),
    ("SAP", re.compile(r"\bSAP\b", re.I), "cloud"),
    ("ServiceNow", re.compile(r"\bServiceNow\b", re.I), "cloud"),
    # Frameworks
    ("React", re.compile(r"\bReact(?:\.?js|JS)?\b", re.I), "framework"),
    ("Angular", re.compile(r"\bAngular\b", re.I), "framework"),
    ("Vue", re.compile(r"\bVue(?:\.?js)?\b", re.I), "framework"),
    ("Node.js", re.compile(r"\bNode(?:\.?js|JS)\b", re.I), "framework"),
    ("Django", re.compile(r"\bDjango\b", re.I), "framework"),
    ("Flask", re.compile(r"\bFlask\b", re.I), "framework"),
    ("Spring", re.compile(r"\bSpring\s*Boot\b|\bSpring\b", re.I), "framework"),
    ("Rails", re.compile(r"\bRails\b|\bRuby on Rails\b", re.I), "framework"),
    ("Next.js", re.compile(r"\bNext(?:\.?js|JS)\b", re.I), "framework"),
    (".NET", re.compile(r"\b\.NET\s*(?:Core|Framework)?\b", re.I), "framework"),
    ("FastAPI", re.compile(r"\bFastAPI\b", re.I), "framework"),
    ("GraphQL", re.compile(r"\bGraphQL\b", re.I), "framework"),
    ("REST API", re.compile(r"\bREST(?:ful)?\s*API\b", re.I), "framework"),
    # DevOps & Infra
    ("Docker", re.compile(r"\bDocker\b", re.I), "devops"),
    ("Kubernetes", re.compile(r"\bKubernetes\b|\bK8s\b", re.I), "devops"),
    ("Terraform", re.compile(r"\bTerraform\b", re.I), "devops"),
    ("Jenkins", re.compile(r"\bJenkins\b", re.I), "devops"),
    ("CI/CD", re.compile(r"\bCI\s*\/?\s*CD\b", re.I), "devops"),
    ("Linux", re.compile(r"\bLinux\b", re.I), "devops"),
    ("Git", re.compile(r"\bGit\b(?!Hub|Lab)", re.I), "devops"),
    ("GitHub", re.compile(r"\bGitHub\b", re.I), "devops"),
    ("Ansible", re.compile(r"\bAnsible\b", re.I), "devops"),
    ("Prometheus", re.compile(r"\bPrometheus\b", re.I), "devops"),
    ("Grafana", re.compile(r"\bGrafana\b", re.I), "devops"),
    # AI/ML
    ("Machine Learning", re.compile(r"\bMachine Learning\b|\bML\b", re.I), "ai"),
    ("Deep Learning", re.compile(r"\bDeep Learning\b", re.I), "ai"),
    ("TensorFlow", re.compile(r"\bTensorFlow\b", re.I), "ai"),
    ("PyTorch", re.compile(r"\bPyTorch\b", re.I), "ai"),
    ("NLP", re.compile(r"\bNLP\b|\bNatural Language Processing\b", re.I), "ai"),
    ("LLM", re.compile(r"\bLLM\b|\bLarge Language Model\b", re.I), "ai"),
    ("GenAI", re.compile(r"\bGenerative AI\b|\bGenAI\b|\bGen AI\b", re.I), "ai"),
    ("Computer Vision", re.compile(r"\bComputer Vision\b|\bCV\b(?=.*(?:model|image|detect))", re.I), "ai"),
    ("OpenAI", re.compile(r"\bOpenAI\b|\bChatGPT\b|\bGPT[-\s]?4\b", re.I), "ai"),
    ("Hugging Face", re.compile(r"\bHugging\s*Face\b", re.I), "ai"),
    # Tools
    ("Jira", re.compile(r"\bJira\b", re.I), "tool"),
    ("Confluence", re.compile(r"\bConfluence\b", re.I), "tool"),
    ("Figma", re.compile(r"\bFigma\b", re.I), "design"),
    ("Sketch", re.compile(r"\bSketch\b", re.I), "design"),
    ("Adobe XD", re.compile(r"\bAdobe\s*XD\b", re.I), "design"),
    ("Photoshop", re.compile(r"\bPhotoshop\b", re.I), "design"),
    ("Illustrator", re.compile(r"\bIllustrator\b", re.I), "design"),
    ("InDesign", re.compile(r"\bInDesign\b", re.I), "design"),
    ("Canva", re.compile(r"\bCanva\b", re.I), "design"),
    ("HubSpot", re.compile(r"\bHubSpot\b", re.I), "tool"),
    ("Marketo", re.compile(r"\bMarketo\b", re.I), "tool"),
    ("Google Analytics", re.compile(r"\bGoogle Analytics\b|\bGA4\b", re.I), "tool"),
    ("SEO", re.compile(r"\bSEO\b", re.I), "tool"),
    ("SEM", re.compile(r"\bSEM\b|\bGoogle Ads\b", re.I), "tool"),
    ("Zendesk", re.compile(r"\bZendesk\b", re.I), "tool"),
    ("Intercom", re.compile(r"\bIntercom\b", re.I), "tool"),
    # Business
    ("Agile", re.compile(r"\bAgile\b", re.I), "business"),
    ("Scrum", re.compile(r"\bScrum\b", re.I), "business"),
    ("Six Sigma", re.compile(r"\bSix Sigma\b", re.I), "business"),
    ("PMP", re.compile(r"\bPMP\b", re.I), "business"),
    ("CPA", re.compile(r"\bCPA\b", re.I), "business"),
    ("GAAP", re.compile(r"\bGAAP\b", re.I), "business"),
    ("SOC 2", re.compile(r"\bSOC\s*2\b", re.I), "security"),
    ("HIPAA", re.compile(r"\bHIPAA\b", re.I), "security"),
    ("SOX", re.compile(r"\bSOX\b|\bSarbanes", re.I), "security"),
    # Security
    ("Cybersecurity", re.compile(r"\bCyber\s*Security\b|\bCybersecurity\b", re.I), "security"),
    ("SIEM", re.compile(r"\bSIEM\b", re.I), "security"),
    ("Penetration Testing", re.compile(r"\bPen\s*Test\b|\bPenetration Testing\b", re.I), "security"),
]


def extract_skills(title: str, description: str) -> str:
    """
    Extract skill/tech tags from job title + description.
    Returns comma-separated skill names (e.g. "Python,AWS,Docker").
    """
    text = f"{title or ''} {description or ''}"
    if not text.strip():
        return ""

    found = []
    seen = set()
    for name, pattern, _cat in SKILL_DB:
        if name not in seen and pattern.search(text):
            seen.add(name)
            found.append(name)
    return ",".join(found)
