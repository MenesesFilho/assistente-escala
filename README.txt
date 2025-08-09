Assistente Gerador de Escalas Inteligentes (San Paolo)
======================================================

O que ele faz?
--------------
- Você envia 3 arquivos: 
    1) MODELO DE ESCALA.xlsx (o seu modelo oficial)
    2) VENDA IGUATEMI DIA (vendas por dia)
    3) VENDA IGUATEMI HORA (vendas por hora)
- Informa abertura, fechamento, nº de funcionários e tipo de escala (5x2 ou 6x1).
- Clica em "Gerar Escala" e baixa um Excel preenchido no padrão da aba "Escala San Paolo",
  além das abas "ESCALA SEMANAL", "Funcionários por Hora (T3)" e "INFORMAÇÕES".

Como rodar localmente
---------------------
1) pip install -r requirements.txt
2) python app.py
3) Abra http://127.0.0.1:5000

Deploy no Render (resumo)
-------------------------
1) Crie um repositório e envie estes arquivos.
2) No Render: New -> Web Service -> conecte o repo.
   - Build Command: pip install -r requirements.txt
   - Start Command: gunicorn app:app --bind 0.0.0.0:$PORT
3) Acesse a URL pública do serviço.
