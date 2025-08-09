from flask import Flask, request, send_file, render_template, abort
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from io import BytesIO

app = Flask(__name__)

SEMANA_DIAS = ['segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira','sábado','domingo']
DIAS_MAP_EN2PT = {'Monday':'segunda-feira','Tuesday':'terça-feira','Wednesday':'quarta-feira',
                  'Thursday':'quinta-feira','Friday':'sexta-feira','Saturday':'sábado','Sunday':'domingo'}

def parse_hour_start(s):
    try:
        h = str(s).split(' ')[0]
        return int(h.split(':')[0])
    except Exception:
        return np.nan

def carregar_vendas_dia(f):
    df = pd.read_excel(f, sheet_name=0, skiprows=3)
    cols = df.columns
    # tentar nomes padrão
    nomes = {'Data':'Data','BALCÃO':'BALCÃO','DELIVERY':'DELIVERY','Total':'Total'}
    # corrigir eventuais diferenças de acentuação/caixa
    m = {c:c for c in cols}
    for k in list(nomes.keys()):
        cand = [c for c in cols if str(c).strip().lower()==k.lower()]
        if cand: m[nomes[k]] = cand[0]
    df = df[[m['Data'], m['BALCÃO'], m['DELIVERY'], m['Total']]].dropna(subset=[m['Total']]).copy()
    df['Data'] = pd.to_datetime(df[m['Data']].astype(str).str[:10], format='%d/%m/%Y', errors='coerce')
    df['DiaSemana'] = df['Data'].dt.day_name().map(DIAS_MAP_EN2PT)
    day_weights = df.groupby('DiaSemana')[m['Total']].mean()
    day_weights = (day_weights / day_weights.sum()).reindex(SEMANA_DIAS).fillna(1/7)
    return day_weights

def carregar_vendas_hora(f, abertura_h, fechamento_h):
    df = pd.read_excel(f, sheet_name=0, skiprows=3)
    # detectar coluna de intervalo/horário
    intervalo_col = 'Intervalo' if 'Intervalo' in df.columns else df.columns[0]
    if 'Vendas' in df.columns and not df['Vendas'].isna().all():
        use_col = 'Vendas'
    elif '% Fat.' in df.columns and not df['% Fat.'].isna().all():
        use_col = '% Fat.'
    else:
        use_col = None

    df['HoraInicial'] = df[intervalo_col].apply(parse_hour_start)
    df['VendasUse'] = df[use_col] if use_col else 1
    df = df.dropna(subset=['HoraInicial']).copy()
    df['HoraInicial'] = df['HoraInicial'].astype(int)

    oper_hours = list(range(abertura_h-1, fechamento_h+1))  # inclui 1h antes
    df = df[df['HoraInicial'].isin(oper_hours)]
    hour_weights = df.groupby('HoraInicial')['VendasUse'].sum()
    if hour_weights.sum() == 0 or hour_weights.isna().all():
        hour_weights = pd.Series({h:1 for h in oper_hours})
    hour_weights = hour_weights / hour_weights.sum()
    return hour_weights

def gerar_escala(modelo_bytes, vendas_dia_bytes, vendas_hora_bytes,
                 loja, num_func, abertura, fechamento, tipo_escala, carga):
    # pesos de dia e hora
    day_w = carregar_vendas_dia(vendas_dia_bytes)
    hour_w = carregar_vendas_hora(vendas_hora_bytes, abertura, fechamento)

    hours = sorted(hour_w.index.tolist())  # 9..22 (se 10-22)
    slots = [(d,h) for d in SEMANA_DIAS for h in hours]

    total_staff_hours = num_func * carga
    base_coverage = 1
    base_total = base_coverage * len(slots)
    extra_available = total_staff_hours - base_total
    slot_weights = {(d,h): (day_w.loc[d] * hour_w.loc[h]) for d,h in slots}
    weight_sum = sum(slot_weights.values()) or 1.0
    slot_extra_float = {k: extra_available * (w/weight_sum) for k,w in slot_weights.items()}
    slot_extra_int = {k: int(np.floor(v)) for k,v in slot_extra_float.items()}
    remainder = int(extra_available - sum(slot_extra_int.values()))
    remainders_sorted = sorted(slot_extra_float.items(), key=lambda kv: kv[1]-np.floor(kv[1]), reverse=True)
    for i in range(remainder):
        slot_extra_int[remainders_sorted[i][0]] += 1
    demand = {(d,h): base_coverage + slot_extra_int[(d,h)] for d,h in slots}
    # mínimo 2 no fechamento
    if (demand.get((SEMANA_DIAS[0], fechamento)) is not None):
        for d in SEMANA_DIAS:
            if demand[(d,fechamento)] < 2:
                demand[(d,fechamento)] = 2

    # montar escala por funcionário (simples/parametrizada)
    employees = [f"Funcionário {i}" for i in range(1, num_func+1)]
    # folgas (5x2 ou 6x1)
    if tipo_escala == "6x1":
        # 1 folga por semana, seg-sex alternando
        weekday_cycle = ['segunda-feira','terça-feira','quarta-feira','quinta-feira','sexta-feira']
        folgas = {emp: (weekday_cycle[(i % len(weekday_cycle))],) for i, emp in enumerate(employees)}
        max_days = 6
    else:  # 5x2
        weekday_pairs = [("segunda-feira","terça-feira"),("terça-feira","quarta-feira"),
                         ("quarta-feira","quinta-feira"),("quinta-feira","sexta-feira")]
        folgas = {emp: weekday_pairs[(i % len(weekday_pairs))] for i, emp in enumerate(employees)}
        max_days = 5

    # turnos com intervalo de 1h (ajustados pela abertura/fechamento)
    shift_types = {
        "abertura":   (abertura-1, abertura+3, abertura+4, abertura+8),  # ex.: 9-13/14-18
        "meio":       (abertura+1, abertura+5, abertura+6, abertura+10), # ex.: 11-15/16-20
        "fechamento": (abertura+4, abertura+8, abertura+9, fechamento+1) # ex.: 14-18/19-23
    }

    needs = {d: {h: int(demand[(d,h)]) for h in hours} for d in SEMANA_DIAS}
    schedule = {emp: {d: None for d in SEMANA_DIAS} for emp in employees}
    emp_days = {emp: 0 for emp in employees}
    emp_hours = {emp: 0 for emp in employees}
    MAX_HORAS_DIA = 10

    # aplica folgas
    for emp in employees:
        offs = folgas[emp]
        for o in offs:
            schedule[emp][o] = "Folga"

    def shift_hours(st): s1,e1,s2,e2 = st; return (e1-s1)+(e2-s2)
    def cover_count(d, st):
        s1,e1,s2,e2 = st
        return sum(max(0, needs[d][h]) for h in range(s1,e1)) + sum(max(0, needs[d][h]) for h in range(s2,e2))
    def apply_shift(d, st):
        s1,e1,s2,e2 = st
        for h in range(s1,e1): needs[d][h] = max(0, needs[d][h]-1)
        for h in range(s2,e2): needs[d][h] = max(0, needs[d][h]-1)
    def shift_to_str(st):
        s1,e1,s2,e2 = st
        return f"{s1:02d}:00 - {e1:02d}:00 / {s2:02d}:00 - {e2:02d}:00"
    def feasible(emp, d, st):
        if schedule[emp][d] not in (None, "Folga"): return False
        dh = shift_hours(st)
        if dh > MAX_HORAS_DIA: return False
        if emp_days[emp] >= max_days: return False
        if emp_hours[emp] + dh > carga: return False
        return True
    def variants(st):
        s1,e1,s2,e2 = st
        var = [(s1,e1,s2,e2)]
        if e2 < fechamento+1: var.append((s1,e1,s2,e2+1))
        return var

    for d in SEMANA_DIAS:
        guard = 0
        while sum(needs[d].values()) > 0 and guard < 1500:
            guard += 1
            best_st, best_cover = None, 0
            for st in shift_types.values():
                for stv in variants(st):
                    c = cover_count(d, stv)
                    if c > best_cover:
                        best_st, best_cover = stv, c
            if best_cover == 0: break
            chosen = None
            for emp in sorted(employees, key=lambda e: (emp_days[e], emp_hours[e])):
                if feasible(emp, d, best_st):
                    chosen = emp; break
            if chosen is None: break
            schedule[chosen][d] = shift_to_str(best_st)
            emp_days[chosen] += 1
            emp_hours[chosen] += shift_hours(best_st)
            apply_shift(d, best_st)

    # preencher modelo
    wb = load_workbook(modelo_bytes)
    # ESCALA SEMANAL B2:H13
    ws_sem = wb['ESCALA SEMANAL']
    for i, emp in enumerate(employees):
        for j, d in enumerate(SEMANA_DIAS):
            val = schedule[emp][d] if schedule[emp][d] else "Folga"
            ws_sem.cell(row=2+i, column=2+j, value=val)

    # Escala San Paolo E18:AI65 (4 linhas por funcionário)
    ws_sp = wb['Escala San Paolo']
    col_start = 5  # E
    row_start = 18
    for idx, emp in enumerate(employees):
        base_row = row_start + idx*4
        for j, d in enumerate(SEMANA_DIAS):
            val = schedule[emp][d]
            if not val or val == "Folga":
                for r in range(4): ws_sp.cell(row=base_row+r, column=col_start+j, value="Folga")
            else:
                p1, p2 = val.split("/")
                s1,e1 = [x.strip() for x in p1.split("-")]
                s2,e2 = [x.strip() for x in p2.split("-")]
                ws_sp.cell(row=base_row+0, column=col_start+j, value=s1)
                ws_sp.cell(row=base_row+1, column=col_start+j, value=e1)
                ws_sp.cell(row=base_row+2, column=col_start+j, value=s2)
                ws_sp.cell(row=base_row+3, column=col_start+j, value=e2)
        # nomes/cargo (só preenche se vazio)
        if ws_sp.cell(row=base_row, column=2).value in (None, ""):
            ws_sp.cell(row=base_row, column=2, value=emp)
        if ws_sp.cell(row=base_row, column=3).value in (None, ""):
            ws_sp.cell(row=base_row, column=3, value="Atendente")

    # Funcionários por Hora (T3) B2:H21
    ws_t3 = wb['Funcionários por Hora (T3)']
    hours_full = list(range(abertura-1, fechamento+1))
    count = pd.DataFrame(0, index=hours_full, columns=SEMANA_DIAS)
    def hour_to_int(hhmm): return int(hhmm.split(':')[0])
    for emp in employees:
        for d in SEMANA_DIAS:
            val = schedule[emp][d]
            if val and val != "Folga":
                try:
                    p1, p2 = val.split('/')
                    s1,e1 = [x.strip() for x in p1.split('-')]
                    s2,e2 = [x.strip() for x in p2.split('-')]
                    for hh in range(hour_to_int(s1), hour_to_int(e1)):
                        if hh in count.index: count.loc[hh, d] += 1
                    for hh in range(hour_to_int(s2), hour_to_int(e2)):
                        if hh in count.index: count.loc[hh, d] += 1
                except: pass
    for i, hh in enumerate(hours_full):
        for j, d in enumerate(SEMANA_DIAS):
            ws_t3.cell(row=2+i, column=2+j, value=int(count.loc[hh, d]))

    # INFORMAÇÕES (preenche somente se os rótulos existirem na coluna A)
    ws_info = wb['INFORMAÇÕES']
    info_map = {
        "Loja": loja,
        "Abertura": f"{abertura:02d}:00",
        "Fechamento": f"{fechamento:02d}:00",
        "Funcionários": str(num_func),
        "Tipo de escala": tipo_escala,
        "Carga horária semanal": f"{carga}h úteis"
    }
    for row in ws_info.iter_rows(min_row=1, max_row=ws_info.max_row, min_col=1, max_col=3):
        label = str(row[0].value) if row[0].value is not None else ""
        if label in info_map:
            row[1].value = info_map[label]

    out = BytesIO()
    wb.save(out); out.seek(0)
    return out

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
        return send_file(out, as_attachment=True, download_name="ESCALA_GERADA.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        return abort(500, f"Erro ao gerar escala: {e}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
