from flask import Flask, request, send_file, render_template, abort
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from io import BytesIO
import math

app = Flask(__name__)

SEMANA_DIAS = ['segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira','sábado','domingo']
DIAS_MAP_EN2PT = {'Monday':'segunda-feira','Tuesday':'terça-feira','Wednesday':'quarta-feira',
                  'Thursday':'quinta-feira','Friday':'sexta-feira','Saturday':'sábado','Sunday':'domingo'}

# -------------------------- Utilitários --------------------------

def parse_hour_start(s):
    try:
        h = str(s).split(' ')[0]
        return int(h.split(':')[0])
    except Exception:
        return np.nan

def carregar_vendas_dia(f):
    try:
        df = pd.read_excel(f, sheet_name=0, skiprows=3)
    except Exception:
        f.seek(0)
        df = pd.read_excel(f, sheet_name=0)
    cols = df.columns
    nomes = {'Data':'Data','BALCÃO':'BALCÃO','DELIVERY':'DELIVERY','Total':'Total'}
    m = {c:c for c in cols}
    for k in list(nomes.keys()):
        cand = [c for c in cols if str(c).strip().lower()==k.lower()]
        if cand: m[nomes[k]] = cand[0]
    use_cols = [m.get('Data'), m.get('BALCÃO'), m.get('DELIVERY'), m.get('Total')]
    use_cols = [c for c in use_cols if c in df.columns]
    df = df[use_cols].dropna(how='all').copy()
    if m.get('Data') in df.columns:
        df['Data'] = pd.to_datetime(df[m['Data']].astype(str).str[:10], errors='coerce', dayfirst=True)
        df['DiaSemana'] = df['Data'].dt.day_name().map(DIAS_MAP_EN2PT)
    if 'DiaSemana' not in df.columns:
        raise ValueError("Planilha de vendas por dia precisa ter coluna Data (dd/mm/aaaa) para calcular o dia da semana.")
    total_col = m.get('Total')
    if total_col not in df.columns:
        poss = [c for c in ['BALCÃO','DELIVERY'] if c in m and m[c] in df.columns]
        if poss:
            df['Total_calc'] = df[m['BALCÃO']].fillna(0) + df[m['DELIVERY']].fillna(0)
            total_col = 'Total_calc'
        else:
            raise ValueError("Não encontrei a coluna 'Total' nem BALCÃO/DELIVERY para somar.")
    day_weights = df.groupby('DiaSemana')[total_col].mean()
    day_weights = (day_weights / day_weights.sum()).reindex(SEMANA_DIAS).fillna(1/7)
    # reforça sábado e domingo (prioridade natural de vendas)
    for d in ['sábado','domingo']:
        if d in day_weights.index:
            day_weights.loc[d] *= 1.15
    day_weights = day_weights / day_weights.sum()
    return day_weights

def carregar_vendas_hora(f, abertura_h, fechamento_h):
    try:
        df = pd.read_excel(f, sheet_name=0, skiprows=3)
    except Exception:
        f.seek(0)
        df = pd.read_excel(f, sheet_name=0)
    intervalo_col = 'Intervalo' if 'Intervalo' in df.columns else df.columns[0]
    use_col = None
    for c in ['Vendas','% Fat.','%Fat.','Percentual','Qtde']:
        if c in df.columns and not df[c].isna().all():
            use_col = c; break
    if use_col is None:
        df['VendasUse'] = 1
    else:
        df['VendasUse'] = df[use_col]
    df['HoraInicial'] = df[intervalo_col].apply(parse_hour_start)
    df = df.dropna(subset=['HoraInicial']).copy()
    df['HoraInicial'] = df['HoraInicial'].astype(int)
    oper_hours = list(range(abertura_h-1, fechamento_h+1))  # inclui 1h antes e a última hora "útil"
    df = df[df['HoraInicial'].isin(oper_hours)]
    hour_weights = df.groupby('HoraInicial')['VendasUse'].sum()
    if hour_weights.sum() == 0 or hour_weights.isna().all():
        hour_weights = pd.Series({h:1 for h in oper_hours})
    hour_weights = hour_weights / hour_weights.sum()
    return hour_weights

# -------------------------- Geração de escala --------------------------

def gerar_escala(modelo_bytes, vendas_dia_bytes, vendas_hora_bytes,
                 loja, num_func, abertura, fechamento, tipo_escala, carga):
    day_w = carregar_vendas_dia(vendas_dia_bytes)
    vendas_dia_bytes.seek(0)
    hour_w = carregar_vendas_hora(vendas_hora_bytes, abertura, fechamento)
    vendas_hora_bytes.seek(0)

    hours = sorted(hour_w.index.tolist())  # (A-1) .. F
    pre_open = abertura - 1
    post_close = fechamento
    h_min = hour_w.idxmin()

    # Demanda alvo por hora/dia
    base_coverage = 2
    needs = {d: {h: base_coverage for h in hours} for d in SEMANA_DIAS}
    for d in SEMANA_DIAS:
        if h_min in needs[d]:  # menor pico do dia = 1
            needs[d][h_min] = 1
        if pre_open in needs[d]:  # 1h antes da abertura = 1
            needs[d][pre_open] = 1
        if post_close in needs[d]:  # hora do fechamento = no mínimo 2
            needs[d][post_close] = max(needs[d][post_close], 2)

    # Funcionários e folgas 5x2 (seg-sex) evitando fds
    employees = [f"Funcionário {i}" for i in range(1, num_func+1)]
    weekday_pairs = [("segunda-feira","terça-feira"),
                     ("terça-feira","quarta-feira"),
                     ("quarta-feira","quinta-feira"),
                     ("quinta-feira","sexta-feira")]
    # Papéis (com proporções seguras p/ garantir fechamento):
    n_close = max(3, math.ceil(num_func * 0.4))
    n_open  = max(2, math.ceil(num_func * 0.25))
    if n_close + n_open > num_func:
        n_close = max(3, num_func - 2)
        n_open = num_func - n_close
    n_mid   = max(1, num_func - n_close - n_open)

    roles = {}
    def off_for_index(idx, total):
        return weekday_pairs[idx % len(weekday_pairs)]

    for i, emp in enumerate(employees[:n_open]):
        roles[emp] = "abertura"
    for i, emp in enumerate(employees[n_open:n_open+n_mid]):
        roles[emp] = "meio"
    for i, emp in enumerate(employees[n_open+n_mid:]):
        roles[emp] = "fechamento"

    folgas = {}
    for idx, emp in enumerate(employees):
        seed = idx + (0 if roles[emp]=="abertura" else (1 if roles[emp]=="meio" else 2))
        folgas[emp] = off_for_index(seed, len(weekday_pairs))

    # Turnos (1h de descanso)
    st_open_8 = (abertura-1, abertura+3, abertura+4, abertura+8)
    st_open_9 = (abertura-1, abertura+3, abertura+4, abertura+9)

    st_mid_8 = (abertura+1, abertura+5, abertura+6, abertura+10)
    st_mid_9 = (abertura+1, abertura+5, abertura+6, abertura+11)

    st_close_8 = (abertura+4, abertura+8, abertura+9, fechamento+1)
    st_close_9 = (abertura+3, abertura+8, abertura+9, fechamento+1)

    def shift_hours(st): 
        s1,e1,s2,e2 = st
        return (e1-s1)+(e2-s2)

    schedule = {emp: {d: "Folga" for d in SEMANA_DIAS} for emp in employees}
    emp_hours = {emp: 0 for emp in employees}
    MAX_HORAS_DIA = 10
    TARGET_SEMANA = 44  # 4x9 + 1x8

    dias_rank = sorted(SEMANA_DIAS, key=lambda d: float(day_w.loc[d]), reverse=True)

    for idx, emp in enumerate(employees):
        papel = roles[emp]
        if papel == "abertura":
            st8, st9 = st_open_8, st_open_9
        elif papel == "meio":
            st8, st9 = st_mid_8, st_mid_9
        else:
            st8, st9 = st_close_8, st_close_9

        off1, off2 = folgas[emp]
        dias_trabalho = [d for d in dias_rank if d not in (off1, off2)][:5]
        if len(dias_trabalho) < 5:
            for d in SEMANA_DIAS:
                if d not in (off1, off2) and d not in dias_trabalho:
                    dias_trabalho.append(d)
                    if len(dias_trabalho) == 5:
                        break

        dias_trabalho = sorted(dias_trabalho, key=lambda d: float(day_w.loc[d]), reverse=True)
        dias_9h = dias_trabalho[:4]
        dia_8h = dias_trabalho[4] if len(dias_trabalho) >= 5 else dias_trabalho[-1]

        for d in dias_trabalho:
            st = st9 if d in dias_9h else st8
            dh = shift_hours(st)
            if dh > MAX_HORAS_DIA:
                st = st8; dh = shift_hours(st8)
            schedule[emp][d] = f"{st[0]:02d}:00 - {st[1]:02d}:00 / {st[2]:02d}:00 - {st[3]:02d}:00"
            emp_hours[emp] += dh

        if emp_hours[emp] < TARGET_SEMANA and dia_8h in dias_trabalho:
            st = st9
            s = f"{st[0]:02d}:00 - {st[1]:02d}:00 / {st[2]:02d}:00 - {st[3]:02d}:00"
            if schedule[emp][dia_8h] != s:
                schedule[emp][dia_8h] = s
                emp_hours[emp] += 1

        schedule[emp][off1] = "Folga"
        schedule[emp][off2] = "Folga"

    # Funcionários por Hora (T3) B2:H...
    ws_t3 = wb['Funcionários por Hora (T3)']
    # horas que realmente existem na operação (usadas para contar)
    hours_full = list(range(abertura-1, fechamento+1))

    # monta contagem (descanso não conta)
    count = pd.DataFrame(0, index=hours_full, columns=SEMANA_DIAS)
    def hour_to_int(hhmm): 
        return int(str(hhmm).split(':')[0])

    for emp in employees:
        for d in SEMANA_DIAS:
            val = schedule[emp][d]
            if val and str(val).lower() != "folga":
                try:
                    p1, p2 = val.split('/')
                    s1,e1 = [x.strip() for x in p1.split('-')]
                    s2,e2 = [x.strip() for x in p2.split('-')]
                    for hh in range(hour_to_int(s1), hour_to_int(e1)):
                        if hh in count.index: 
                            count.loc[hh, d] += 1
                    for hh in range(hour_to_int(s2), hour_to_int(e2)):
                        if hh in count.index: 
                            count.loc[hh, d] += 1
                except Exception:
                    pass

    # alinha as horas com os rótulos existentes na coluna A (ex.: 08:00, 09:00, ...)
    grid_hours = []
    row = 2
    while True:
        label = ws_t3.cell(row=row, column=1).value
        if label is None or str(label).strip() == "":
            break
        try:
            # suporta tanto '08:00' quanto valores datetime/time
            if isinstance(label, str):
                hh = int(label.split(':')[0])
            else:
                hh = int(str(label).split(':')[0])
        except Exception:
            break
        grid_hours.append((row, hh))
        row += 1

    for r, hh in grid_hours:
        for j, d in enumerate(SEMANA_DIAS):
            val = int(count.loc[hh, d]) if hh in count.index else 0
            ws_t3.cell(row=r, column=2+j, value=val)

    # INFORMAÇÕES
    ws_info = wb['INFORMAÇÕES']
    info_map = {
        "Loja": loja,
        "Abertura": f"{abertura:02d}:00",
        "Fechamento": f"{fechamento:02d}:00",
        "Funcionários": str(num_func),
        "Tipo de escala": tipo_escala,
        "Carga horária semanal": f"{carga}h úteis",
        "Observações": "Regras aplicadas: 1h pré-abertura; 1h pós-fechamento; base 2 por hora (1 no menor pico); papéis fixos; 44h/semana (4x9h+1x8h); folgas 5x2 seg-sex; 2 no fechamento; T3 sem intervalos."
    }
    for row in ws_info.iter_rows(min_row=1, max_row=ws_info.max_row, min_col=1, max_col=3):
        label = str(row[0].value) if row[0].value is not None else ""
        if label in info_map:
            row[1].value = info_map[label]

    out = BytesIO()
    wb.save(out); out.seek(0)
    return out

# -------------------------- Rotas --------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/gerar", methods=["POST"])
def gerar():
    try:
        loja = request.form.get("loja", "San Paolo")
        num_func = int(request.form.get("num_func", "10"))
        abertura = int(request.form.get("abertura", "10:00").split(":")[0])
        fechamento = int(request.form.get("fechamento", "22:00").split(":")[0])
        tipo_escala = request.form.get("tipo_escala", "5x2")
        carga = int(request.form.get("carga", "44"))

        modelo = request.files.get("modelo")
        vendas_dia = request.files.get("vendas_dia")
        vendas_hora = request.files.get("vendas_hora")
        if not modelo or not vendas_dia or not vendas_hora:
            return abort(400, "Envie os três arquivos (modelo, vendas por dia e vendas por hora).")

        out = gerar_escala(modelo, vendas_dia, vendas_hora, loja, num_func, abertura, fechamento, tipo_escala, carga)
        return send_file(out, as_attachment=True, download_name="ESCALA_GERADA.xlsx",
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        return abort(500, f"Erro ao gerar escala: {e}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
