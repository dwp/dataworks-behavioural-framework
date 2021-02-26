FROM dwpdigital/python-boto-behave

RUN apk add libffi-dev openssl-dev python3-dev rust

RUN mkdir /src
RUN mkdir /resources

COPY assume_role.sh /
COPY requirements.txt /resources/
COPY src/ /src/

WORKDIR /resources
ENV CRYPTOGRAPHY_DONT_BUILD_RUST=1
ENV PATH=$PATH:/root/.local/bin
RUN pip install -r requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org --user

WORKDIR /src
RUN chmod -R +x runners/

WORKDIR /src/runners
ENV TEST_RUN_NAME=default_test_run_name
ENV E2E_FEATURE_TAG_FILTER=
ENV RUNNER_SCRIPT=./run-ci.sh

ENTRYPOINT ["sh"]
CMD ["./run-ci.sh"]
