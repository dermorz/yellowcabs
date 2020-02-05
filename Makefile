.IGNORE: clean
clean:
	@rm requirements.txt requirements-test.txt

.IGNORE: detox
detox:
	@rm -rf .tox/

requirements: clean requirements.txt requirements-test.txt

requirements.txt:
	@pipenv lock -r > requirements.txt

requirements-test.txt:
	@pipenv lock -r --dev > requirements-test.txt

.PHONY: fmt
fmt:
	@pipenv run black tests yellowcabs

.PHONY: run-tests
run-tests: fmt requirements
	@tox

