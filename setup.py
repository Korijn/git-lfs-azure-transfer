from setuptools import setup


setup(
    name="git-lfs-azure-transfer",
    version="0.1.0",
    scripts=['git_lfs_azure_transfer.py'],
    install_requires=['azure-storage'],
    description="Custom git-lfs transfer agent for Azure Blob Storage",
    url="https://github.com/korijn/git-lfs-azure-transfer",
    keywords="git lfs azure blob storage transfer agent",
)
