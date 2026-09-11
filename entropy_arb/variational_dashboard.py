"""Compact Rich dashboard for Variational + Lighter RH automation."""
from __future__ import annotations

import asyncio
import time

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def _money(value, signed=True):
    if value is None:
        return Text("—", style="dim")
    style = "bold green" if value > 0 else ("bold red" if value < 0 else "")
    return Text(f"${value:+,.4f}" if signed else f"${value:,.2f}", style=style)


class VariationalDashboard:
    def __init__(self, view, log_buffer, log_file):
        self.view = view
        self.log_buffer = log_buffer
        self.log_file = log_file
        self.console = Console()

    async def run(self):
        with Live(self._safe_render(), console=self.console, refresh_per_second=4,
                  screen=True) as live:
            while not self.view.stop.is_set():
                live.update(self._safe_render())
                try:
                    await asyncio.wait_for(self.view.stop.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass
        self.console.print(
            f"Variational + RH 已停止 · 成交循环 {self.view.trades} · "
            f"累计毛收益 ${self.view.total_profit:+.4f} · "
            f"累计成交额 ${self.view.total_volume:,.2f} · 日志 {self.log_file}")

    def _safe_render(self):
        try:
            return self._render()
        except Exception as error:
            return Panel(f"看板渲染错误: {error!r}\n完整日志: {self.log_file}",
                         style="bold red")

    def _render(self):
        v = self.view
        up = int(time.time() - v.start_ts)
        mode = Text(" 实盘 ", style="white on dark_green") if v.live \
            else Text(" 模拟 ", style="black on yellow")
        status = Text(" 运行中 ", style="bold white on green") if v.ready \
            else Text(" 等待行情 ", style="black on yellow")
        header = Table.grid(expand=True)
        header.add_column()
        header.add_column(justify="right")
        header.add_row(Text.assemble(("Variational × RH  ", "bold"),
                                     (v.symbol, "bold cyan")),
                       Text.assemble(mode, "  ", status,
                                     f"  {up // 3600}:{up % 3600 // 60:02d}:{up % 60:02d}"))

        markets = Table(box=box.SIMPLE_HEAD, expand=True)
        for name, justify in (("交易所", "left"), ("买一", "right"),
                              ("卖一", "right"), ("持仓", "right"),
                              ("名义金额", "right")):
            markets.add_column(name, justify=justify)
        vm = v.var_book.mid()
        rm = v.rh.book.mid()
        markets.add_row("Variational RFQ", self._px(v.var_book.best_bid()),
                        self._px(v.var_book.best_ask()), f"{v.var_pos:+.8g}",
                        f"${abs(v.var_pos) * vm:,.2f}" if vm else "—")
        markets.add_row("Lighter RH", self._px(v.rh.book.best_bid()),
                        self._px(v.rh.book.best_ask()), f"{v.rh_pos:+.8g}",
                        f"${abs(v.rh_pos) * rm:,.2f}" if rm else "—")

        signal = Table.grid(padding=(0, 2))
        signal.add_column(style="dim")
        signal.add_column()
        signal.add_row("卖出 Variational edge", self._edge(v.sell_edge))
        signal.add_row("买入 Variational edge", self._edge(v.buy_edge))
        signal.add_row("当前信号", Text(v.signal, style="bold green" if v.signal != "-" else "dim"))
        signal.add_row("下一触发条件", Text(v.next_condition, style="cyan"))
        signal.add_row("开仓金额", Text(f"${v.notional:,.2f}"))
        signal.add_row("连接状态", Text("正常" if v.ready else v.missing,
                                       style="green" if v.ready else "yellow"))

        session = Table.grid(padding=(0, 2))
        session.add_column(style="dim")
        session.add_column(justify="right")
        session.add_row("最近一笔毛收益", _money(v.last_profit))
        session.add_row("累计毛收益", _money(v.total_profit))
        session.add_row("最近一笔成交额", _money(v.last_volume, signed=False))
        session.add_row("累计成交额", _money(v.total_volume, signed=False))
        session.add_row("成交循环", Text(str(v.trades)))
        session.add_row("净敞口", Text(f"{v.var_pos + v.rh_pos:+.8g}",
                                      style="bold red" if abs(v.var_pos + v.rh_pos) > 1e-6 else "dim"))
        if v.recorder is not None:
            session.add_row("已写分钟数", Text(str(v.recorder.rows_written)))
            session.add_row("采集文件", Text(v.recorder.path, style="dim"))

        trades = Table(box=box.SIMPLE_HEAD, expand=True)
        for name, justify in (("时间", "left"), ("动作", "left"),
                              ("数量", "right"), ("毛收益", "right"),
                              ("成交额", "right")):
            trades.add_column(name, justify=justify)
        if v.recent_trades:
            for row in reversed(v.recent_trades):
                trades.add_row(row["time"], row["action"], f'{row["qty"]:.8g}',
                               f'${row["profit"]:+.4f}', f'${row["volume"]:,.2f}')
        else:
            trades.add_row("暂无成交", "", "", "", "")

        events = Text()
        for level, line in list(self.log_buffer.lines)[-5:]:
            events.append(line + "\n", style="red" if level >= 40 else
                          ("yellow" if level >= 30 else "dim cyan"))
        if not events.plain:
            events.append("—", style="dim")
        return Group(Panel(header, box=box.ROUNDED, padding=(0, 1)),
                     Panel(markets, title="行情与持仓", box=box.ROUNDED),
                     Panel(signal, title="套利信号", box=box.ROUNDED),
                     Panel(session, title="会话统计", box=box.ROUNDED),
                     Panel(trades, title="最近成交", box=box.ROUNDED),
                     Panel(events, title=f"日志事件 · {self.log_file}", box=box.ROUNDED))

    @staticmethod
    def _px(value):
        return f"{value:,.6f}" if value is not None else "—"

    @staticmethod
    def _edge(value):
        return Text(f"{value:+.2f} bps" if value is not None else "—",
                    style="bold cyan" if value is not None else "dim")
