import setuptools
from setuptools import find_packages

__description__ = "The scrapper gets monthly crude volume data for UStrade."
__repo_name__ = "ea_csb_etl"

requirements = [
    'selenium',
    'pandas',
    'sqlalchemy',
    'psycopg2-binary',
    'sqlalchemy',
    'python-dotenv',
]
setuptools.setup(
    name=__repo_name__,
    version='1.0.0',
    description=__description__,
    url=f'https://github.com/energyaspects/{__repo_name__}.git',
    author='ByteIQ Data Engineering Team',
    author_email='',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
    ],
    install_requires=requirements,
    package_data={"": ["*"]},
    entry_points={
        'console_scripts': [
            "cpi_etl = macro.cpi_main.main:main",
            "gdp_etl = macro.gdp_main.main:main",
            "pcpi_etl = macro.pcpi_main.main:main",
            "retail_sales_etl = macro.retail_sales_main.main:main",
        ],
    },
)