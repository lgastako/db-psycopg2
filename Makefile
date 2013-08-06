all:
	@echo ""
	@echo "u:pload to pypi"
	@echo ""

upload:
	python setup.py sdist upload

u: upload
