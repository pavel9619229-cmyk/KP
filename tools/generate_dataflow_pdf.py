from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
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


def arrow_red(c, x1, y1, x2, y2):
    c.setStrokeColor(colors.HexColor("#C23636"))
    c.setLineWidth(1.6)
    c.setDash(6, 4)
    c.line(x1, y1, x2, y2)

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
    c = canvas.Canvas(OUTPUT, pagesize=A4)
    width, height = A4

    c.setFillColor(colors.HexColor("#F4F6FB"))
    c.rect(0, 0, width, height, stroke=0, fill=1)

    c.setFillColor(colors.HexColor("#182236"))
    c.setFont("Helvetica-Bold", 15)
    c.drawString(15 * mm, height - 15 * mm, "Блок-схема: движение данных из 1С в UI (portrait)")

    c.setFont("Helvetica", 10)
    c.setFillColor(colors.HexColor("#4E5D7A"))
    c.drawString(15 * mm, height - 21 * mm, "Вертикальный поток: 1С -> backend -> runtime -> API/UI")

    # Portrait layout in mm
    h = 16 * mm
    h_big = 20 * mm
    w_main = 82 * mm
    x_main = 64 * mm
    x_center = x_main + w_main / 2

    y1 = 242 * mm
    y2 = 218 * mm
    y3 = 194 * mm
    y4 = 166 * mm
    y5 = 140 * mm

    box(c, x_main, y1, w_main, h, "1) 1С OData", ["Документы КП, реквизиты, комментарии"])
    box(c, x_main, y2, w_main, h, "2) Backend fetch", ["Чтение из 1С и сбор сырых строк"])
    box(c, x_main, y3, w_main, h, "3) Нормализация", ["Правила статусов и enrichment"])
    box(c, x_main, y4, w_main, h_big, "4) Runtime consistency", ["Выбор authoritative snapshot"])
    warning_inline(c, x_main + 2 * mm, y4 + 2 * mm, w_main - 4 * mm, 8 * mm, [
        "Источник затирания: перезапись _cached_rows",
    ])
    box(c, x_main, y5, w_main, h, "5) Оперативный кэш", ["_cached_rows (память процесса)"])

    y_cache = 96 * mm
    box(c, 14 * mm, y_cache, 48 * mm, 18 * mm, "LOCAL (сервер)", ["локальная FS, data/*"])
    box(c, 66 * mm, y_cache, 54 * mm, 18 * mm, "6) Runtime-файлы", ["cache + meta + current"])
    box(c, 124 * mm, y_cache, 50 * mm, 18 * mm, "7) Версии", ["runtime_versions/*"])
    box(c, 124 * mm, 68 * mm, 50 * mm, 18 * mm, "8) GitHub publish", ["snapshot + pointer"])

    box(c, 66 * mm, 40 * mm, 76 * mm, 18 * mm, "9) API + UI", ["/api/kp/all + /ws/kp"])
    box(c, 58 * mm, 16 * mm, 92 * mm, 18 * mm, "10) Действие в UI", ["endpoint -> PATCH в 1С"])

    # Target overlay callouts
    c.setStrokeColor(colors.HexColor("#C23636"))
    c.setLineWidth(1.1)
    c.setFillColor(colors.HexColor("#FFF2F2"))
    c.roundRect(14 * mm, 40 * mm, 48 * mm, 18 * mm, 2 * mm, stroke=1, fill=1)
    c.roundRect(14 * mm, 16 * mm, 48 * mm, 18 * mm, 2 * mm, stroke=1, fill=1)
    c.setFillColor(colors.HexColor("#8B2020"))
    c.setFont("Helvetica-Bold", 8)
    c.drawString(16 * mm, 52 * mm, "TARGET: Queue/Lock")
    c.drawString(16 * mm, 48 * mm, "1 процесс одновременно")
    c.drawString(16 * mm, 28 * mm, "TARGET: verify 5 = 8")
    c.drawString(16 * mm, 24 * mm, "иначе stop next step")

    # Main vertical arrows
    arrow(c, x_center, y1, x_center, y2 + h)
    arrow(c, x_center, y2, x_center, y3 + h)
    arrow(c, x_center, y3, x_center, y4 + h_big)
    arrow(c, x_center, y4, x_center, y5 + h)

    # Cache branch arrows
    arrow(c, x_center, y4, 62 * mm, y_cache + 9 * mm)
    arrow(c, 62 * mm, y_cache + 9 * mm, 66 * mm, y_cache + 9 * mm)
    arrow(c, 120 * mm, y_cache + 9 * mm, 124 * mm, y_cache + 9 * mm)
    arrow(c, 149 * mm, y_cache, 149 * mm, 86 * mm)

    # To UI arrows
    arrow(c, x_center, y5, 104 * mm, 58 * mm)
    arrow(c, 149 * mm, 68 * mm, 131 * mm, 58 * mm)

    # Feedback arrows
    arrow(c, 104 * mm, 40 * mm, 104 * mm, 34 * mm, dashed=True)
    arrow(c, 58 * mm, 25 * mm, 20 * mm, 25 * mm, dashed=True)
    arrow(c, 20 * mm, 25 * mm, 20 * mm, 250 * mm, dashed=True)
    arrow(c, 20 * mm, 250 * mm, 64 * mm, 250 * mm, dashed=True)

    # Target overlay arrows
    arrow_red(c, 66 * mm, 49 * mm, 62 * mm, 49 * mm)
    arrow_red(c, 38 * mm, 40 * mm, 38 * mm, 34 * mm)
    arrow_red(c, 38 * mm, 34 * mm, 58 * mm, 34 * mm)
    arrow_red(c, 38 * mm, 25 * mm, 66 * mm, y_cache + 9 * mm)

    c.setFillColor(colors.HexColor("#334566"))
    c.setFont("Helvetica", 8)
    c.drawString(15 * mm, 12 * mm, "Черный: текущий поток. Красный: проектируемый слой queue/lock и verify 5=8 перед следующим шагом.")
    c.setFont("Helvetica", 7)
    t = c.beginText(15 * mm, 9 * mm)
    t.setLeading(8)
    for line in [
        "Длительность full refresh зависит от: глубины цикла, скорости 1С OData, объема строк, ретраев/таймаутов stage2.5/3/4,",
        "скорости publish в GitHub и ожидания lock. Практика: от десятков секунд до минут; hard ceiling manual refresh ~21 мин.",
    ]:
        t.textLine(line)
    c.drawText(t)

    c.showPage()
    c.save()


if __name__ == "__main__":
    main()
