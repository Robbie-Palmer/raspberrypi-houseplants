from setuptools import setup

package_name = 'houseplants'
with open('requirements.txt', 'r', encoding='utf-8') as requirements_file:
    requirements = requirements_file.readlines()
setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    description='A Houseplant Alerting System on Kafka on a Raspberry Pi',
    install_requires=requirements,
    package_data={package_name: ['avro/*.avsc', 'configs/*', '*.sql', '*.json']}
)
