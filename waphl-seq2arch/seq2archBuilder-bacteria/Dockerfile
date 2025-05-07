# ---------------------------
# Stage 1: Java Builder
# ---------------------------
    FROM amazonlinux:2 AS java-builder
    # Install OpenJDK 11 using amazon-linux-extras
    RUN yum install -y tar
    
    RUN yum update -y && \
        amazon-linux-extras install java-openjdk11 -y && \
        yum clean all
    
    # Create a symlink with a known name for the Java installation.
    # This finds the installed directory that starts with "java-11-openjdk" and creates a symlink named "java-11-openjdk".
    RUN export JAVA_DIR=$(find /usr/lib/jvm -maxdepth 1 -type d -name "java-11-openjdk*" | head -n 1) && \
        echo "JAVA_DIR is $JAVA_DIR" && \
        ln -s "$JAVA_DIR" /usr/lib/jvm/java-11-openjdk && \
        ls -la /usr/lib/jvm
    
    # ---------------------------
    # Stage 2: SeqeraKit Builder
    # ---------------------------
    FROM public.ecr.aws/biocontainers/seqerakit:0.5.4--pyhdfd78af_0 AS seqerakit-builder

    
    # ---------------------------
    # Stage 3: Final App Image
    # ---------------------------
    FROM public.ecr.aws/lambda/python:3.13 AS app
    
    # Copy the Java installation from the java-builder stage.
    COPY --from=java-builder /usr/lib/jvm/java-11-openjdk /usr/lib/jvm/java-11-openjdk
    COPY --from=java-builder /usr/bin/tar /usr/bin/tar
    
    # Copy the required binaries from the seqerakit-builder stage.
    COPY --from=seqerakit-builder /usr/local/bin/tw /usr/local/bin/tw
    COPY --from=seqerakit-builder /usr/local/bin/seqerakit /usr/local/bin/seqerakit
    #COPY --from=seqerakit-builder /usr/local/bin/tw-jar.jar /usr/local/bin/tw-jar.jar
    
    RUN curl -L -o /usr/local/bin/tw-jar.jar https://github.com/seqeralabs/tower-cli/releases/download/v0.11.4/tw-jar.jar
    # Set the Java environment variables so the runtime can locate its libraries.
    ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk
    ENV PATH="${JAVA_HOME}/bin:${PATH}"
    ENV LD_LIBRARY_PATH="${JAVA_HOME}/lib/jli:${LD_LIBRARY_PATH}"
    

    # # Install Python dependencies into the Lambda task directory
    COPY requirements.txt ${LAMBDA_TASK_ROOT}
    RUN pip3 install --upgrade pip
    RUN pip3 install -r requirements.txt


    # # Copy your application code into the Lambda task directory
    COPY lambda_function.py ${LAMBDA_TASK_ROOT}
    COPY run_clean_seqera.sh ${LAMBDA_TASK_ROOT}
    COPY clean_seqera.py ${LAMBDA_TASK_ROOT}
    COPY profiles.py ${LAMBDA_TASK_ROOT}
    COPY run_clean_seqera_dryrun.sh ${LAMBDA_TASK_ROOT}


    CMD [ "lambda_function.handler" ]


    # Test that the Java runtime works, tw can run, and that tar is installed correctly.
    FROM app AS test
    RUN java -version
    RUN tw -h
    RUN tar --usage
    