.DEFAULT_GOAL := help

install:
	@echo "Installing dependency ${LIB}"
	poetry add ${LIB}

delete:
	@echo "Uninstalling dependency ${LIB}"
	poetry remove ${LIB}

update:
	@echo "Updating dependency ${LIB}"
	poetry update ${LIB}

git-push:
	@echo "Push master branch ${BRANCH}"
	git push --set-upstream origin ${BRANCH}
