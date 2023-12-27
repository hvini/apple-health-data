import setuptools

setuptools.setup(
    name='apple-health-data-dataflow-pipeline',
    version='0.0.1',
    description='Apple Health Data Pipeline Package.',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "apache-beam[gcp]==2.52.0",
        "xmltodict==0.13.0",
    ],
)
