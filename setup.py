from setuptools import setup, find_packages
from pathlib import Path

# Load README.md content
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="rag-mongo-logger",
    version="0.1.3",  # bump this for each reupload
    description="Async buffered logger with MongoDB and PostgreSQL support for your RAG applications",
    long_description=long_description,
    long_description_content_type="text/markdown",  # very important!
    author="Siddhesh Dosi",
    author_email="siddheshdosi106@gmail.com",
    packages=find_packages(),
    install_requires=[
        "pymongo",
        "SQLAlchemy",
    ],
    python_requires=">=3.10",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
