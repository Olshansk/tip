.SILENT:

.PHONY: help
## This help screen
help:
	printf "Available targets\n\n"
	awk '/^[a-zA-Z\-\_0-9]+:/ { \
		helpMessage = match(lastLine, /^## (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")-1); \
			helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
			printf "%-30s %s\n", helpCommand, helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.PHONY: generate_html
## Generate HTML of InvestigateData notebook
generate_html:
	jupyter nbconvert BackTest_InvestigateData.ipynb  --no-input --to html
	mv BackTest_InvestigateData.ipynb index.html
# pandoc BackTest_InvestigateData.html -t latex -o DataInvestigation.pdf
