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


def arrow(c, x1, y1, x2, y2):
    c.setStrokeColor(colors.HexColor("#2F3E5F"))
    c.setLineWidth(1.4)
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
    c.drawString(15 * mm, height - 21 * mm, "1С OData -> backend обработка -> кэши -> API -> интерфейс")

    # Layout
    y_top = height - 62 * mm
    h_box = 30 * mm
    w_box = 54 * mm
    gap = 10 * mm

    x1 = 12 * mm
    x2 = x1 + w_box + gap
    x3 = x2 + w_box + gap
    x4 = x3 + w_box + gap
    x5 = x4 + w_box + gap

    box(c, x1, y_top, w_box, h_box, "1) 1С OData", [
        "Источник документов КП,", "комментариев и реквизитов",
    ])
    box(c, x2, y_top, w_box, h_box, "2) Fetch в backend", [
        "Запросы к 1С,", "нормализация полей",
    ])
    box(c, x3, y_top, w_box, h_box, "3) Обработка", [
        "Расчет статусов,", "enrichment правил",
    ])
    box(c, x4, y_top, w_box, h_box, "4) Оперативный кэш", [
        "_cached_rows", "в памяти процесса",
    ])
    box(c, x5, y_top, w_box, h_box, "5) Runtime-кэш", [
        "data/kp_runtime_cache.json", "meta/current + versions",
    ])

    y_mid = y_top - 46 * mm
    box(c, x3, y_mid, w_box, h_box, "6) GitHub publish", [
        "Версии и current-pointer", "публикуются в GitHub",
    ])
    box(c, x4 + 20 * mm, y_mid, w_box, h_box, "7) API + UI", [
        "GET /api/kp/all, WS /ws/kp", "Отрисовка карточек и статусов",
    ])

    y_low = y_mid - 46 * mm
    box(c, x2, y_low, w_box + 20 * mm, h_box, "8) Действие пользователя", [
        "Кнопка в UI вызывает endpoint", "например: /api/kp/process/send-to-client",
    ])
    box(c, x1 + 10 * mm, y_low, w_box + 20 * mm, h_box, "9) Обратный PATCH в 1С", [
        "Обновляется комментарий/статус", "и цикл повторяется",
    ])

    # Arrows top row
    cy = y_top + h_box / 2
    arrow(c, x1 + w_box, cy, x2, cy)
    arrow(c, x2 + w_box, cy, x3, cy)
    arrow(c, x3 + w_box, cy, x4, cy)
    arrow(c, x4 + w_box, cy, x5, cy)

    # Downward to API/UI
    arrow(c, x3 + w_box / 2, y_top, x3 + w_box / 2, y_mid + h_box)
    arrow(c, x4 + w_box / 2 + 20 * mm, y_top, x4 + w_box / 2 + 20 * mm, y_mid + h_box)
    arrow(c, x3 + w_box, y_mid + h_box / 2, x4 + 20 * mm, y_mid + h_box / 2)

    # Feedback path
    arrow(c, x4 + 20 * mm + w_box / 2, y_mid, x2 + 20 * mm, y_low + h_box)
    arrow(c, x2 + 20 * mm, y_low + h_box / 2, x1 + 10 * mm + w_box + 20 * mm, y_low + h_box / 2)
    arrow(c, x1 + 10 * mm + w_box / 2, y_low + h_box, x1 + w_box / 2, y_top)

    c.setFillColor(colors.HexColor("#334566"))
    c.setFont("Helvetica", 8)
    c.drawString(15 * mm, 10 * mm, "Файл создан автоматически: dataflow_1c_to_ui_diagram.pdf")

    c.showPage()
    c.save()


if __name__ == "__main__":
    main()
