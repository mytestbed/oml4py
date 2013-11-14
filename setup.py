from distutils.core import setup
setup(name='oml4py',
      version='2.10.4',
      author = "Fraida Fund",
      author_email = "ffund01@students.poly.edu",
      maintainer_email = "oml-user@lists.nicta.com.au",
      description = ("An OML client module for Python"),
      url = "http://github.com/mytestbed/oml4py",
      download_url = "http://pypi.python.org/pypi/oml4py",
      py_modules=['oml4py'],
      license = "MIT",
      classifiers=[
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS Independent',
          'Topic :: Scientific/Engineering',
      ],
      long_description=open('README.txt', 'rt').read(),
      )
