import os, re, json
from datetime import datetime, timedelta
import pytz
import pandas as pd
import gspread
from google.auth import default
import requests

# ===================== 初期設定（環境変数） =====================
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "URL_NOT_SET")
SPREADSHEET_URL   = os.environ.get("SPREADSHEET_URL", "URL_NOT_SET")
SHEET_YM          = os.environ.get("SHEET_YM")  # 例: "202510"。未指定なら今月自動

BOT_NAME      = os.environ.get("BOT_NAME", "pjscbf 広告効果")
INTRO_MESSAGE = os.environ.get("INTRO_MESSAGE", f"pjscbfの<{SPREADSHEET_URL}|広告効果>共有です！")
MENTION_TEXT  = os.environ.get("MENTION_TEXT", "")  # 例 "@taku_sasaki @xxx"

# ラベル（必要あればENVで上書き可能）
MEDIA_TOTAL_LABEL   = os.environ.get("MEDIA_TOTAL_LABEL", "全体")
LABEL_COST_BASE     = os.environ.get("LABEL_COST_BASE", "消化金額")
LABEL_INSTALLS_BASE = os.environ.get("LABEL_INSTALLS_BASE", "インストール")
LABEL_PU_ADJUST_BASE   = os.environ.get("LABEL_PU_ADJUST_BASE", "課金者数(adjust)")
LABEL_REVENUE_ADJ_BASE = os.environ.get("LABEL_REVENUE_ADJ_BASE", "課金金額(adjust)")

# ===== ROAS: 売上*0.7/1.1/配信金額（%表示） =====
REVENUE_SPLIT = float(os.environ.get("REVENUE_SPLIT", "0.7"))  # 0.7
FEE_DIVISOR   = float(os.environ.get("FEE_DIVISOR", "1.1"))    # 1.1

# ===================== Google Sheets から取得 =====================
print("[INFO] Google認証開始（spreadsheets.readonly）")
creds, _ = default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
gc = gspread.authorize(creds)

print(f"[INFO] スプレッドシートを開きます: {SPREADSHEET_URL}")
sh = gc.open_by_url(SPREADSHEET_URL)

# シート名：指定が無ければ今月の "YYYYMM"
jst = pytz.timezone('Asia/Tokyo')
today = datetime.now(jst).date()
sheet_name = SHEET_YM if SHEET_YM else today.strftime("%Y%m")
print(f"[INFO] 対象シート: {sheet_name}")
ws = sh.worksheet(sheet_name)

data = ws.get_all_values()
if not data or not data[0]:
    raise RuntimeError("シートが空です。ヘッダを確認してください。")
df = pd.DataFrame(data[1:], columns=data[0])

# ===================== ユーティリティ =====================
def norm(s: str) -> str:
    if s is None: return ""
    s = str(s)
    # 全角括弧→半角、スペース削除、小文字化
    s = s.replace("（", "(").replace("）", ")").replace("　", " ").strip().lower()
    s = re.sub(r"\s+", "", s)
    return s

def find_key(index_like, prefer_exact_list, fuzzy_keywords_list):
    """
    index_like: Index（項目名の配列）
    prefer_exact_list: 優先して拾いたい“正規名”候補（例: 課金金額(adjust)）
    fuzzy_keywords_list: フォールバックで 'すべて含む' 判定に使うキーワード群（AND）
    """
    original = list(index_like)
    mapping = {norm(k): k for k in original}

    # 1) 厳密一致（正規名）
    for c in prefer_exact_list:
        c_norm = norm(c)
        if c_norm in mapping:
            return mapping[c_norm]

    # 2) 部分一致（AND 条件）
    for k in original:
        k_norm = norm(k)
        if all(norm(kw) in k_norm for kw in fuzzy_keywords_list):
            return k

    return None

def to_number_series(series: pd.Series) -> pd.Series:
    s = series.copy()
    s = s.astype(str).str.replace('[^0-9.\-]', '', regex=True).replace('', '0')
    return pd.to_numeric(s, errors='coerce').fillna(0)

# ===================== 日付列の検出 =====================
yesterday = today - timedelta(days=1)
first_day = today.replace(day=1)

def find_date_columns(frame: pd.DataFrame, start_date: datetime, end_date: datetime):
    cols = []
    cur = start_date
    while cur <= end_date:
        pat = re.compile(rf"^{cur.month}/{cur.day}(?:\(|$)")
        for c in frame.columns:
            if pat.match(c):
                cols.append(c)
                break
        cur += timedelta(days=1)
    return cols

mtd_cols = find_date_columns(df, first_day, yesterday)
yday_col = next((c for c in df.columns if re.match(rf"^{yesterday.month}/{yesterday.day}(?:\(|$)", c)), None)
print(f"[INFO] MTD列: {mtd_cols}")
print(f"[INFO] 昨日列: {yday_col}")

# ===================== ロング化 & 数値化 =====================
if "媒体名" in df.columns and "項目" in df.columns:
    df_long = df.melt(id_vars=['媒体名', '項目'], var_name='日付', value_name='値')
else:
    # 先頭2列をID列代用
    id_vars = list(df.columns[:2])
    print(f"[WARN] '媒体名'/'項目'が見つからないため代替ID列を使用: {id_vars}")
    df_long = df.melt(id_vars=id_vars, var_name='日付', value_name='値')
    if '媒体名' not in df_long.columns: df_long.rename(columns={id_vars[0]: '媒体名'}, inplace=True)
    if '項目' not in df_long.columns:   df_long.rename(columns={id_vars[1]: '項目'}, inplace=True)

# 全体フィルタ（列が無い場合はスキップ）
if '媒体名' in df_long.columns:
    df_long_total = df_long[df_long['媒体名'] == MEDIA_TOTAL_LABEL].copy()
else:
    df_long_total = df_long.copy()

df_long_total['値'] = to_number_series(df_long_total['値'])

# ===================== 集計（MTD / 昨日） =====================
df_month = df_long_total[df_long_total['日付'].isin(mtd_cols)]
mtd_series = df_month.groupby('項目')['値'].sum()

yday_series = pd.Series(dtype=float)
if yday_col:
    yday_series = df_long_total[df_long_total['日付'] == yday_col].set_index('項目')['値']

# ===================== キー解決（adjust最優先＋汎用フォールバック） =====================
# 課金者数：課金者数(adjust) を最優先、無ければ '課金者数' っぽい
pu_key_mtd  = find_key(mtd_series.index,
                       prefer_exact_list=[LABEL_PU_ADJUST_BASE, "課金者数(adjust)"],
                       fuzzy_keywords_list=["課金者数"])
pu_key_yday = find_key(yday_series.index if len(yday_series) else mtd_series.index,
                       prefer_exact_list=[LABEL_PU_ADJUST_BASE, "課金者数(adjust)"],
                       fuzzy_keywords_list=["課金者数"])

# 課金金額：課金金額(adjust) 最優先、無ければ “課金金額” を含むもの or “売上” を含むもの
rev_key_mtd = find_key(mtd_series.index,
                       prefer_exact_list=[LABEL_REVENUE_ADJ_BASE, "課金金額(adjust)"],
                       fuzzy_keywords_list=["課金","金額"]) or \
              find_key(mtd_series.index, [], ["売上"])
rev_key_yday = find_key(yday_series.index if len(yday_series) else mtd_series.index,
                        prefer_exact_list=[LABEL_REVENUE_ADJ_BASE, "課金金額(adjust)"],
                        fuzzy_keywords_list=["課金","金額"]) or \
               find_key(yday_series.index if len(yday_series) else mtd_series.index, [], ["売上"])

# 消化金額・インストール（adjust優先）
cost_key_mtd = find_key(mtd_series.index,
                        prefer_exact_list=[LABEL_COST_BASE],
                        fuzzy_keywords_list=["消化","金額"]) or \
               find_key(mtd_series.index, [], ["広告費"])
inst_key_mtd = find_key(mtd_series.index,
                        prefer_exact_list=[f"{LABEL_INSTALLS_BASE}(adjust)", "インストール(adjust)"],
                        fuzzy_keywords_list=["インストール"]) or \
               find_key(mtd_series.index, [LABEL_INSTALLS_BASE, "インストール"], ["インストール"])

# 昨日も同様（無ければMTD側キーを使い回し）
cost_key_yday = find_key(yday_series.index if len(yday_series) else mtd_series.index,
                         prefer_exact_list=[LABEL_COST_BASE],
                         fuzzy_keywords_list=["消化","金額"]) or cost_key_mtd
inst_key_yday = find_key(yday_series.index if len(yday_series) else mtd_series.index,
                         prefer_exact_list=[f"{LABEL_INSTALLS_BASE}(adjust)", "インストール(adjust)"],
                         fuzzy_keywords_list=["インストール"]) or \
                find_key(yday_series.index if len(yday_series) else mtd_series.index,
                         [LABEL_INSTALLS_BASE, "インストール"], ["インストール"]) or inst_key_mtd

print("[MATCH] MTD:", dict(cost=cost_key_mtd, installs=inst_key_mtd, pu=pu_key_mtd, revenue=rev_key_mtd))
print("[MATCH] YDAY:", dict(cost=cost_key_yday, installs=inst_key_yday, pu=pu_key_yday, revenue=rev_key_yday))

def metrics_from(series: pd.Series, keys: dict) -> dict:
    if series is None or len(series) == 0:
        series = pd.Series(dtype=float)
    getv = lambda k: float(series.get(k, 0)) if k else 0.0

    cost     = getv(keys.get("cost"))
    installs = getv(keys.get("installs"))
    pu       = getv(keys.get("pu"))
    revenue  = getv(keys.get("revenue"))

    # 自前で再計算（シートのCPI/CPA等は参照しない）
    cpa = cost / pu if pu > 0 else 0
    cpi = cost / installs if installs > 0 else 0
    pu_rate = (pu / installs * 100) if installs > 0 else 0
    arppu = revenue / pu if pu > 0 else 0

    # ====== ROAS: 売上 * REVENUE_SPLIT / FEE_DIVISOR / 配信金額（%） ======
    roas_base = revenue * REVENUE_SPLIT / FEE_DIVISOR
    roas = (roas_base / cost * 100) if cost > 0 else 0

    return dict(cost=cost, installs=installs, pu=pu, revenue=revenue,
                cpa=cpa, cpi=cpi, pu_rate=pu_rate, arppu=arppu, roas=roas)

mtd = metrics_from(mtd_series, dict(cost=cost_key_mtd, installs=inst_key_mtd, pu=pu_key_mtd, revenue=rev_key_mtd))
yday = metrics_from(yday_series, dict(cost=cost_key_yday, installs=inst_key_yday, pu=pu_key_yday, revenue=rev_key_yday))

# ===================== 表示整形 =====================
def fmt(m: dict) -> dict:
    return {
        "cost":     f"¥{m['cost']:,.0f}",
        "installs": f"{m['installs']:,.0f}",
        "pu":       f"{m['pu']:,.0f}",
        "cpi":      f"¥{m['cpi']:,.0f}",
        "cpa":      f"¥{m['cpa']:,.0f}",
        "pu_rate":  f"{m['pu_rate']:.2f}%",
        "revenue":  f"¥{m['revenue']:,.0f}",
        "arppu":    f"¥{m['arppu']:,.0f}",
        "roas":     f"{m['roas']:.2f}%"
    }

mtd_f = fmt(mtd)
yday_f = fmt(yday)

weekday_ja = ["月","火","水","木","金","土","日"]
yday_label = f"{yesterday.month}/{yesterday.day}({weekday_ja[yesterday.weekday()]})"

# ===================== Slack 投稿 =====================
msg_tpl = (
    "費用：{cost}\n"
    "インストール：{installs}\n"
    "課金者数：{pu}\n"
    "CPI：{cpi}\n"
    "CPA：{cpa}\n"
    "課金率：{pu_rate}\n"
    "課金金額：{revenue}\n"
    "ARPPU：{arppu}\n"
    "ROAS：{roas}"
)

text = ""
if MENTION_TEXT:
    text += MENTION_TEXT + "\n"
text += f"{INTRO_MESSAGE}\n\n"
text += f"▼{today.month}月進捗（{first_day.month}/{first_day.day}-{yesterday.month}/{yesterday.day}時点）\n{msg_tpl.format(**mtd_f)}\n\n"
text += f"▼昨日進捗({yday_label})\n{msg_tpl.format(**yday_f)}"

print("[INFO] Slackへ送信します…")
resp = requests.post(SLACK_WEBHOOK_URL, data=json.dumps({
    "text": text,
    "username": BOT_NAME,
    "icon_emoji": ":bar_chart:"
}))
if resp.status_code == 200:
    print("🎉 Slack投稿 成功")
else:
    print(f"[ERROR] Slack投稿 失敗 status={resp.status_code} body={resp.text}")
