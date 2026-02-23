from setuptools import setup, find_packages

setup(
    name="ceph_like",
    version="1.0.0",
    description="Ceph-like Distributed Storage System",
    packages=find_packages(),
    install_requires=[
        "kazoo",
    ],
    python_requires=">=3.7",
)
