FROM python:3.12.10 AS dist
LABEL maintainer="793875613@qq.com"
LABEL copyright="joinsunsoft-锦翰科技"
LABEL software="LLM-RAG"

#代码添加到code文件夹
ADD ./dataflow/*.py /code/dataflow/
ADD ./requirements.txt /code/requirements.txt

#ADD requirements.txt /code/

WORKDIR /code

#安装支持 -i 换国内pip源
RUN /usr/local/bin/python -m pip install --upgrade pip -i https://pypi.mirrors.ustc.edu.cn/simple/
#RUN pip install pipreqs -i https://pypi.mirrors.ustc.edu.cn/simple/
RUN pip install pyinstaller -i https://pypi.mirrors.ustc.edu.cn/simple/
#RUN pipreqs ./ --encoding=utf8 --force
RUN pip install -r requirements.txt -i https://pypi.mirrors.ustc.edu.cn/simple/
RUN pyinstaller --onefile rag.py -n llm
RUN cp /code/dist/llm /code/

VOLUME /code


ENTRYPOINT  ["./llm"]
