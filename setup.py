from setuptools import setup, find_packages


def parse_requirements(requirements):
    with open(requirements) as f:
        return [l.strip('\n') for l in f if l.strip('\n') and not l.startswith('#')]


requirements = parse_requirements('requirements.txt')

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='osf_pigeon',
    version='0.0.1',
    description='A utility for archiving osf storage projects at archive.org',
    long_description=long_description,
    author='Center for Open Science',
    author_email='contact@cos.io',
    install_requires=requirements,
    url='https://github.com/CenterForOpenScience/osf-pigeon',
    packages=find_packages(exclude=("tests*", )),
    zip_safe=False,
    classifiers=[
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
    ],
    provides=['osf_pigeon']
)
