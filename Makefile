# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
.PHONY: all
all:
	$(error please pick a target)

.PHONY: lint
lint:
	./venv/bin/python -m flake8 storey

.PHONY: test
test:
	find storey -name '*.pyc' -exec rm {} \;
	find tests -name '*.pyc' -exec rm {} \;
	./venv/bin/python -m pytest --ignore=integration -rf -v .

.PHONY: integration
integration:
	find integration -name '*.pyc' -exec rm {} \;
	./venv/bin/python -m pytest -rf -v integration

.PHONY: env
env:
	python3 -m venv venv
	./venv/bin/python -m pip install -r requirements.txt

.PHONY: dev-env
dev-env: env
	./venv/bin/python -m pip install -r dev-requirements.txt
