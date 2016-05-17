from setuptools import setup, find_packages
setup(
    name="txamqpwrap",
    version="0.1",
    packages=find_packages(exclude='test'),
    py_modules=['txamqpwrap'],
    install_requires=['txamqp>=0.1'],
    author="k1nkreet",
    author_email="polyakovskiy.ilya@gmail.com",
    description="Classes for wrapping txAMQP functions",
    license="MIT",
    url="https://github.com/k1nkreet/txamqpwrap",
)
