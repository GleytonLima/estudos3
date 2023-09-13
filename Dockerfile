# Use uma imagem base do Python para Linux
FROM python:3.7

# Instale a biblioteca ujson usando pip
RUN pip install ujson

RUN mkdir /host
RUN cp -r /usr/local/lib/python3.7/site-packages/ujson* /host/

# Defina um comando de teste (opcional)
CMD ["python", "-c", "import ujson; print(ujson.dumps({'message': 'Hello, world!'}))"]
