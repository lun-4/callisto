from setuptools import setup

setup(
    name="callisto",
    version="0.1",
    py_modules=["callisto"],
    install_requires=[],
    entry_points="""
        [console_scripts]
        callisto=callisto:cli
    """,
)
