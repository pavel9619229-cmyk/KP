from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas


OUTPUT = r"C:\Users\Server\Documents\API\dataflow_1c_to_ui_diagram.pdf"


def box(c, x, y, w, h, title, lines):
    c.setStrokeColor(colors.HexColor("#2F3E5F"))
    c.setLineWidth(1.2)
    c.setFillColor(colors.white)
    c.roundRect(x, y, w, h, 6 * mm, stroke=1, fill=1)

    c.setFillColor(colors.HexColor("#182236"))
    c.setFont("Helvetica-Bold", 11)
    c.drawString(x + 4 * mm, y + h - 8 * mm, title)

    c.setFont("Helvetica", 9)
    t = c.beginText(x + 4 * mm, y + h - 14 * mm)
    t.setFillColor(colors.HexColor("#2A3A5A"))
    t.setLeading(11)
    for line in lines:
        t.textLine(line)
    c.drawText(t)


def warning_inline(c, x, y, w, h, text):
    c.setStrokeColor(colors.HexColor("#B44A4A"))
    c.setLineWidth(1.1)
    c.setFillColor(colors.HexColor("#FFF3F2"))
    c.roundRect(x, y, w, h, 2.2 * mm, stroke=1, fill=1)
    c.setFillColor(colors.HexColor("#7D1F1F"))
    c.setFont("Helvetica-Bold", 7.8)
    t = c.beginText(x + 2.2 * mm, y + h - 3.2 * mm)
    t.setLeading(8.6)
    for line in text:
        t.textLine(line)
    c.drawText(t)


def arrow(c, x1, y1, x2, y2, dashed=False):
    c.setStrokeColor(colors.HexColor("#2F3E5F"))
    c.setLineWidth(1.5)
    if dashed:
        c.setDash(6, 4)
    else:
        c.setDash()
    c.line(x1, y1, x2, y2)

    # arrow head
    size = 4 * mm
    if x2 != x1:
        sign = 1 if x2 > x1 else -1
        c.line(x2, y2, x2 - sign * size, y2 + size / 2)
        c.line(x2, y2, x2 - sign * size, y2 - size / 2)
    else:
        sign = 1 if y2 > y1 else -1
        c.line(x2, y2, x2 + size / 2, y2 - sign * size)
        c.line(x2, y2, x2 - size / 2, y2 - sign * size)
    c.setDash()


def main():
    c = canvas.Canvas(OUTPUT, pagesize=landscape(A4))
    width, height = landscape(A4)

    c.setFillColor(colors.HexColor("#F4F6FB"))
    c.rect(0, 0, width, height, stroke=0, fill=1)

    c.setFillColor(colors.HexColor("#182236"))
    c.setFont("Helvetica-Bold", 16)
    c.drawString(15 * mm, height - 15 * mm, "Блок-схема: движение данных из 1С в UI")

    c.setFont("Helvetica", 10)
    c.setFillColor(colors.HexColor("#4E5D7A"))
    c.drawString(15 * mm, height - 21 * mm, "Четкий поток: 1С -> backend -> snapshot -> API -> UI + обратная запись в 1С")

    # Layout
    h_box = 30 * mm
    w_small = 42 * mm
    w_mid = 48 * mm
    w_big = 58 * mm

    y_main = height - 62 * mm
    x1 = 12 * mm
    x2 = 58 * mm
    x3 = 109 * mm
    x4 = 160 * mm
    x5 = 222 * mm

    box(c, x1, y_main, w_small, h_box, "1) 1С OData", ["Источник документов КП", "и комментариев"])
    box(c, x2, y_main, w_small, h_box, "2) Backend fetch", ["Чтение из 1С", "первичная сборка"])
    box(c, x3, y_main, w_small, h_box, "3) Нормализация", ["Правила статусов", "enrichment"])
    box(c, x4, y_main, w_big, h_box, "4) Runtime consistency", ["Сравнение local/GitHub", "выбор authoritative snapshot"])
    warning_inline(c, x4 + 2 * mm, y_main + 2 * mm, w_big - 4 * mm, 11 * mm, [
        "ЗДЕСЬ ИСТОЧНИК ЗАТИРАНИЯ:",
        "перезапись _cached_rows старым snapshot",
    ])
    box(c, x5, y_main, w_mid, h_box, "5) _cached_rows", ["Оперативный кэш", "в памяти процесса"])

    y_cache = y_main - 44 * mm
    x_local = 84 * mm
    x6 = 140 * mm
    x7 = 196 * mm
    x8 = 252 * mm
    box(c, x_local, y_cache, 52 * mm, h_box, "LOCAL (сервер)", ["Локальная FS Render", "директория data/*"])
    box(c, x6, y_cache, w_mid, h_box, "6) Runtime-файлы", ["kp_runtime_cache.json", "meta/current"])
    box(c, x7, y_cache, w_mid, h_box, "7) Версии", ["runtime_versions/*", "version pointer"])
    box(c, x8, y_cache, w_mid, h_box, "8) GitHub publish", ["push snapshot", "и current-pointer"])

    y_ui = y_cache - 44 * mm
    x9 = 246 * mm
    x10 = 160 * mm
    box(c, x9, y_ui, 56 * mm, h_box, "9) API + UI", ["/api/kp/all, /ws/kp", "карточки в dashboard"])
    box(c, x10, y_ui, 72 * mm, h_box, "10) Действие в UI", ["Кнопка вызывает endpoint", "backend делает PATCH в 1С"])

    # Main line arrows
    cy = y_main + h_box / 2
    arrow(c, x1 + w_small, cy, x2, cy)
    arrow(c, x2 + w_small, cy, x3, cy)
    arrow(c, x3 + w_small, cy, x4, cy)
    arrow(c, x4 + w_big, cy, x5, cy)

    # Cache branch arrows
    arrow(c, x4 + 6 * mm, y_main, x_local + 52 * mm, y_cache + h_box / 2)
    arrow(c, x_local + 52 * mm, y_cache + h_box / 2, x6, y_cache + h_box / 2)
    arrow(c, x5 - 6 * mm, y_main, x6 + 10 * mm, y_cache + h_box)
    arrow(c, x6 + w_mid, y_cache + h_box / 2, x7, y_cache + h_box / 2)
    arrow(c, x7 + w_mid, y_cache + h_box / 2, x8, y_cache + h_box / 2)
    arrow(c, x8 + w_mid / 2, y_cache, x9 + 16 * mm, y_ui + h_box)

    # Feedback arrows
    arrow(c, x9, y_ui + h_box / 2, x10 + 72 * mm, y_ui + h_box / 2, dashed=True)
    arrow(c, x10, y_ui + h_box / 2, x1 + 12 * mm, y_ui + h_box / 2, dashed=True)
    arrow(c, x1 + 12 * mm, y_ui + h_box / 2, x1 + 12 * mm, y_main + h_box, dashed=True)

    c.setFillColor(colors.HexColor("#334566"))
    c.setFont("Helvetica", 8)
    c.drawString(15 * mm, 10 * mm, "LOCAL на схеме: отдельный блок в кэш-слое (локальная FS сервера, data/*).")

    c.showPage()
    c.save()


if __name__ == "__main__":
    main()
