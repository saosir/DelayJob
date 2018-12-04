install_oa:
	pip install -r requirements.txt -i http://pypi.open.oa.com/simple/ --extra-index-url http://pypi.open.oa.com/simple/ --trusted-host pypi.open.oa.com
lint:
	flake8
cov:
	pytest --cov-report term --cov-report html  --cov=DelayJob tests/
test:
	py.test -v