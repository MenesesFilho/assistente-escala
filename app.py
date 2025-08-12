from flask import Flask, request, send_file, render_template, abort
import os
from io import BytesIO

from openai import OpenAI
from openpyxl import load_workbook

# ================= OpenAI / Assistants setup =================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ASSISTANT_ID   = os.getenv("ASSISTANT_ID", "asst_XXXX_REPLACE_ME")   # ID do seu Assistente (asst_...)
OPENAI_PROJECT = os.getenv("OPENAI_PROJECT")                          # ID do Projeto (proj_...)

if not OPENAI_API_KEY:
    print("WARNING: OPENAI_API_KEY não está definido. Configure no Render > Environment.")

# Instancia o client apontando para o projeto correto (evita 404 "No assistant found")
client = OpenAI(api_key=OPENAI_API_KEY, project=OPENAI_PROJECT) if OPENAI_PROJECT else OpenAI(api_key=OPENAI_API_KEY)

# ================= Flask app =================
app = Flask(__name__)

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


def wait_for_run(thread_id: str, run_id: str, timeout_s: int = 360):
    """Aguarda o Assistente concluir o processamento."""
    import time
    start = time.time()
    while True:
        run = client.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
        if run.status in ("completed", "failed", "cancelled", "expired"):
            return run
        if time.time() - start > timeout_s:
            raise TimeoutError("Tempo excedido aguardando o Assistente concluir o run.")
        time.sleep(1.2)


def to_openai_upload_tuple(fs):
    """
    Normaliza uploads para extensões aceitas pela OpenAI.
    - Converte .xltx/.xlsm para .xlsx usando openpyxl
    - Mantém .xlsx e .csv
    """
    name = fs.filename or "arquivo.xlsx"
    lower = name.lower()

    # Sempre volte o ponteiro do stream
    try:
        fs.stream.seek(0)
    except Exception:
        pass

    if lower.endswith(".xltx") or lower.endswith(".xlsm"):
        # Converte para .xlsx em memória
        try:
            wb = load_workbook(fs.stream, data_only=True)
        except Exception as e:
            raise RuntimeError(f"Falha ao abrir '{name}' para conversão: {e}")
        out = BytesIO()
        wb.save(out)
        out.seek(0)
        newname = os.path.splitext(name)[0] + ".xlsx"
        return (newname, out)

    # extensões aceitas diretamente
    if lower.endswith(".xlsx") or lower.endswith(".csv"):
        return (name, fs.stream)

    # Caso outra planilha antiga (.xls) apareça, informe claramente
    raise RuntimeError(f"Extensão não suportada: {name}. Salve como .xlsx ou .csv.")


@app.route("/gerar", methods=["POST"])
def gerar():
    try:
        # --------- 1) Parâmetros ---------
        loja = request.form.get("loja", "San Paolo")
        num_func = request.form.get("num_func", "10")
        abertura = request.form.get("abertura", "10:00")
        fechamento = request.form.get("fechamento", "22:00")
        tipo_escala = request.form.get("tipo_escala", "5x2")
        carga = request.form.get("carga", "44")

        modelo = request.files.get("modelo")
        vendas_dia = request.files.get("vendas_dia")
        vendas_hora = request.files.get("vendas_hora")
        if not modelo or not vendas_dia or not vendas_hora:
            return abort(400, "Envie os três arquivos (modelo, vendas por dia e vendas por hora).")

        if not ASSISTANT_ID or ASSISTANT_ID.endswith("REPLACE_ME"):
            return abort(500, "ASSISTANT_ID não configurado. Defina no Render > Environment.")

        # --------- 2) Upload para a OpenAI ---------
        up_modelo = client.files.create(file=to_openai_upload_tuple(modelo), purpose="assistants")
        up_dia    = client.files.create(file=to_openai_upload_tuple(vendas_dia), purpose="assistants")
        up_hora   = client.files.create(file=to_openai_upload_tuple(vendas_hora), purpose="assistants")

        # --------- 3) Thread + mensagem ---------
        thread = client.beta.threads.create()
        prompt_usuario = (
            "Gerar a escala no MODELO OFICIAL seguindo as Diretrizes Oficiais – Especialista de Escalas San Paolo.\n\n"
            f"Loja: {loja}\n"
            f"Número de funcionários: {num_func}\n"
            f"Horário de abertura: {abertura}\n"
            f"Horário de fechamento: {fechamento}\n"
            f"Tipo de escala: {tipo_escala}\n"
            f"Carga horária semanal (úteis): {carga}\n\n"
            "Arquivos anexos:\n"
            "- MODELO DE ESCALA.xlsx\n- VENDAS por DIA.xlsx\n- VENDAS por HORA.xlsx\n\n"
            "Retorne um único arquivo Excel final com as abas 'Escala San Paolo' (E18:AI65), 'ESCALA SEMANAL' "
            "(turnos no formato HH:MM - HH:MM / HH:MM - HH:MM + Total por Dia), "
            "'Funcionários por Hora (T3)' (contagem real por hora) e 'INFORMAÇÕES' (parâmetros). "
            "As três abas devem manter dados idênticos, mudando apenas a visualização. "
            "Respeite: 44h/semana por funcionário, máx. 10h úteis/dia, 1h antes da abertura e 1h após fechamento, "
            "mín. 1 na abertura e 2 no fechamento, folgas entre seg-sex (preferencialmente sequenciais) e reforço nos picos."
        )

        client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=prompt_usuario,
            attachments=[
                {"file_id": up_modelo.id, "tools": [{"type": "code_interpreter"}]},
                {"file_id": up_dia.id,    "tools": [{"type": "code_interpreter"}]},
                {"file_id": up_hora.id,   "tools": [{"type": "code_interpreter"}]},
            ]
        )

        # --------- 4) Run ---------
        run = client.beta.threads.runs.create(
            thread_id=thread.id,
            assistant_id=ASSISTANT_ID,
        )

        # --------- 5) Esperar ---------
        run = wait_for_run(thread.id, run.id)
        if run.status != "completed":
            return abort(500, f"Run não completou. Status: {run.status}")

        # --------- 6) Capturar arquivo gerado ---------
        msgs = client.beta.threads.messages.list(thread_id=thread.id, order="desc", limit=20)
        output_file_id = None
        for m in msgs.data:
            for part in m.content:
                if getattr(part, "type", None) == "file_path":
                    output_file_id = part.file_id
                    break
            if output_file_id:
                break

        if not output_file_id:
            steps = client.beta.threads.runs.steps.list(thread_id=thread.id, run_id=run.id, order="desc", limit=20)
            for st in steps.data:
                details = getattr(st, "step_details", None)
                tool_calls = getattr(details, "tool_calls", []) if details else []
                for tc in tool_calls:
                    if getattr(tc, "type", "") == "code_interpreter":
                        outputs = getattr(tc.code_interpreter, "outputs", []) or []
                        for out in outputs:
                            if getattr(out, "type", "") == "image":
                                continue
                            fid = getattr(out, "file_id", None)
                            if fid:
                                output_file_id = fid
                                break
                    if output_file_id:
                        break
                if output_file_id:
                    break

        if not output_file_id:
            return abort(500, "Não foi possível localizar o arquivo Excel gerado pelo Assistente.")

        # --------- 7) Baixar ---------
        file_bytes = client.files.content(output_file_id).read()
        return send_file(
            BytesIO(file_bytes),
            as_attachment=True,
            download_name="ESCALA_GERADA.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return abort(500, f"Erro ao gerar escala via Assistants API: {e}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
