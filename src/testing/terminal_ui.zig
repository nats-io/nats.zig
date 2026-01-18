/// Terminal UI for interactive progress display.
/// Uses 256 color mode theme
/// Falls back to simple output when not a TTY.
const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const File = std.Io.File;
const Io = std.Io;

pub const TerminalUI = struct {
    is_tty: bool,
    stderr: File,
    io: Io,
    spinner_idx: u8 = 0,

    /// ASCII spinner characters for progress animation.
    pub const SPINNER = [_][]const u8{ "|", "/", "-", "\\" };

    // ANSI escape codes - control
    pub const ESC_CLEAR_LINE = "\x1b[2K";
    pub const ESC_CURSOR_COL1 = "\x1b[1G";
    pub const ESC_RESET = "\x1b[0m";
    pub const ESC_BOLD = "\x1b[1m";

    // 256-color "Slate Blue" theme
    pub const ESC_HEADER = "\x1b[38;5;111m"; // table headers
    pub const ESC_PROGRESS = "\x1b[38;5;245m"; // [1/5]
    pub const ESC_RUN = "\x1b[38;5;240m"; // Run 3/10
    pub const ESC_SPINNER = "\x1b[38;5;75m"; // spinner
    pub const ESC_RUNNING = "\x1b[38;5;147m"; // "Running..."
    pub const ESC_OK = "\x1b[38;5;114m"; // [OK]
    pub const ESC_FAIL = "\x1b[38;5;203m"; // [FAIL]
    pub const ESC_RATE = "\x1b[38;5;117m"; // rate numbers
    pub const ESC_UNIT = "\x1b[38;5;243m"; // "msg/s"
    pub const ESC_BORDER = "\x1b[38;5;238m"; // box borders
    pub const ESC_TITLE = "\x1b[38;5;255m"; // title text

    // Box drawing characters (UTF-8 encoded)
    pub const BOX_H = "\xe2\x94\x80"; // ─ horizontal
    pub const BOX_V = "\xe2\x94\x82"; // │ vertical
    pub const BOX_TL = "\xe2\x94\x8c"; // ┌ top-left
    pub const BOX_TR = "\xe2\x94\x90"; // ┐ top-right
    pub const BOX_BL = "\xe2\x94\x94"; // └ bottom-left
    pub const BOX_BR = "\xe2\x94\x98"; // ┘ bottom-right
    pub const BOX_LT = "\xe2\x94\x9c"; // ├ left-tee
    pub const BOX_RT = "\xe2\x94\xa4"; // ┤ right-tee
    pub const BOX_TT = "\xe2\x94\xac"; // ┬ top-tee
    pub const BOX_BT = "\xe2\x94\xb4"; // ┴ bottom-tee
    pub const BOX_X = "\xe2\x94\xbc"; // ┼ cross

    /// Initialize the terminal UI.
    pub fn init(io: Io) TerminalUI {
        const stderr = File.stderr();
        return .{
            .is_tty = stderr.isTty(io) catch false,
            .stderr = stderr,
            .io = io,
        };
    }

    /// Clear current line and move cursor to column 1.
    pub fn clearLine(self: *TerminalUI) void {
        if (!self.is_tty) return;
        self.stderr.writeStreamingAll(
            self.io,
            ESC_CLEAR_LINE ++ ESC_CURSOR_COL1,
        ) catch {};
    }

    /// Write text with specified color (no-op if not TTY).
    fn writeColor(self: *TerminalUI, color: []const u8, text: []const u8) void {
        if (self.is_tty) self.stderr.writeStreamingAll(self.io, color) catch {};
        self.stderr.writeStreamingAll(self.io, text) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
    }

    /// Write colored text (green for success).
    pub fn writeGreen(self: *TerminalUI, text: []const u8) void {
        self.writeColor(ESC_OK, text);
    }

    /// Write colored text (red for error).
    pub fn writeRed(self: *TerminalUI, text: []const u8) void {
        self.writeColor(ESC_FAIL, text);
    }

    /// Get current spinner character and advance.
    pub fn spin(self: *TerminalUI) []const u8 {
        const char = SPINNER[self.spinner_idx];
        self.spinner_idx = @intCast((self.spinner_idx + 1) % SPINNER.len);
        return char;
    }

    /// Show progress: "[1/5] Client  Run 3/10 ⠹ Running..."
    pub fn showRunning(
        self: *TerminalUI,
        client_idx: usize,
        total_clients: usize,
        name: []const u8,
        run: usize,
        total_runs: u32,
    ) void {
        assert(client_idx > 0 and client_idx <= total_clients);
        assert(run > 0 and run <= total_runs);
        assert(name.len > 0);

        self.clearLine();

        // [1/5] in gray
        var prog_buf: [16]u8 = undefined;
        const prog = std.fmt.bufPrint(&prog_buf, "  [{d}/{d}] ", .{
            client_idx,
            total_clients,
        }) catch return;
        self.writeColor(ESC_PROGRESS, prog);

        // Client name in bold
        self.writeColor(ESC_BOLD, name);

        // Padding
        var pad_buf: [12]u8 = undefined;
        const pad_len = if (name.len < 10) 10 - name.len else 0;
        @memset(pad_buf[0..pad_len], ' ');
        self.stderr.writeStreamingAll(self.io, pad_buf[0..pad_len]) catch {};

        // Run X/Y in dark gray
        var run_buf: [16]u8 = undefined;
        const run_str = std.fmt.bufPrint(&run_buf, " Run {d}/{d} ", .{
            run,
            total_runs,
        }) catch return;
        self.writeColor(ESC_RUN, run_str);

        // Spinner in sky blue
        self.writeColor(ESC_SPINNER, self.spin());

        // "Running..." in light slate
        self.writeColor(ESC_RUNNING, " Running...");

        self.stderr.writeStreamingAll(self.io, "\r") catch {};
    }

    /// Show success: "[1/5] Client  [OK]  rate"
    pub fn showSuccess(
        self: *TerminalUI,
        client_idx: usize,
        total_clients: usize,
        name: []const u8,
        rate_str: []const u8,
    ) void {
        assert(client_idx > 0 and client_idx <= total_clients);
        assert(name.len > 0);

        self.clearLine();

        // [1/5] in gray
        var prog_buf: [16]u8 = undefined;
        const prog = std.fmt.bufPrint(&prog_buf, "  [{d}/{d}] ", .{
            client_idx,
            total_clients,
        }) catch return;
        self.writeColor(ESC_PROGRESS, prog);

        // Client name in bold
        self.writeColor(ESC_BOLD, name);

        // Padding
        var pad_buf: [12]u8 = undefined;
        const pad_len = if (name.len < 10) 10 - name.len else 0;
        @memset(pad_buf[0..pad_len], ' ');
        self.stderr.writeStreamingAll(self.io, pad_buf[0..pad_len]) catch {};

        // [OK] in soft green
        self.writeColor(ESC_OK, " [OK]");

        self.stderr.writeStreamingAll(self.io, "  ") catch {};

        // Rate in bright sky, extract number vs unit
        self.writeColor(ESC_RATE, rate_str);

        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Show failure: "[1/5] Client  [FAIL] reason"
    pub fn showFailure(
        self: *TerminalUI,
        client_idx: usize,
        total_clients: usize,
        name: []const u8,
        reason: []const u8,
    ) void {
        assert(client_idx > 0 and client_idx <= total_clients);
        assert(name.len > 0);

        self.clearLine();

        // [1/5] in gray
        var prog_buf: [16]u8 = undefined;
        const prog = std.fmt.bufPrint(&prog_buf, "  [{d}/{d}] ", .{
            client_idx,
            total_clients,
        }) catch return;
        self.writeColor(ESC_PROGRESS, prog);

        // Client name in bold
        self.writeColor(ESC_BOLD, name);

        // Padding
        var pad_buf: [12]u8 = undefined;
        const pad_len = if (name.len < 10) 10 - name.len else 0;
        @memset(pad_buf[0..pad_len], ' ');
        self.stderr.writeStreamingAll(self.io, pad_buf[0..pad_len]) catch {};

        // [FAIL] in soft red
        self.writeColor(ESC_FAIL, " [FAIL]");

        self.stderr.writeStreamingAll(self.io, " ") catch {};
        self.writeColor(ESC_RUN, reason);
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Print a section header in slate blue.
    pub fn printHeader(self: *TerminalUI, title: []const u8) void {
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
        self.writeColor(ESC_HEADER, title);
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Print plain text.
    pub fn print(self: *TerminalUI, text: []const u8) void {
        self.stderr.writeStreamingAll(self.io, text) catch {};
    }

    /// Print a horizontal line of specified width.
    pub fn printHLine(self: *TerminalUI, width: usize) void {
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        for (0..width) |_| {
            self.stderr.writeStreamingAll(self.io, BOX_H) catch {};
        }
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
    }

    /// Print top border: ┌────────────────────┐
    pub fn printBoxTop(self: *TerminalUI, width: usize) void {
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_TL) catch {};
        for (0..width - 2) |_| {
            self.stderr.writeStreamingAll(self.io, BOX_H) catch {};
        }
        self.stderr.writeStreamingAll(self.io, BOX_TR) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Print bottom border: └────────────────────┘
    pub fn printBoxBottom(self: *TerminalUI, width: usize) void {
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_BL) catch {};
        for (0..width - 2) |_| {
            self.stderr.writeStreamingAll(self.io, BOX_H) catch {};
        }
        self.stderr.writeStreamingAll(self.io, BOX_BR) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Print a boxed line: │ text                    │
    pub fn printBoxLine(
        self: *TerminalUI,
        width: usize,
        text: []const u8,
    ) void {
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_V) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, " ") catch {};
        self.stderr.writeStreamingAll(self.io, text) catch {};

        // Calculate padding needed
        const text_len = text.len;
        const content_width = width - 4; // minus │ and spaces
        if (text_len < content_width) {
            for (0..content_width - text_len) |_| {
                self.stderr.writeStreamingAll(self.io, " ") catch {};
            }
        }
        self.stderr.writeStreamingAll(self.io, " ") catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_V) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Print a boxed line with colored text.
    pub fn printBoxLineColored(
        self: *TerminalUI,
        width: usize,
        color: []const u8,
        text: []const u8,
    ) void {
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_V) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, " ") catch {};
        self.writeColor(color, text);

        // Calculate padding needed
        const text_len = text.len;
        const content_width = width - 4;
        if (text_len < content_width) {
            for (0..content_width - text_len) |_| {
                self.stderr.writeStreamingAll(self.io, " ") catch {};
            }
        }
        self.stderr.writeStreamingAll(self.io, " ") catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_V) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }

    /// Segment for multi-colored box line.
    pub const Segment = struct { color: []const u8, text: []const u8 };

    /// Print a boxed line with multiple colored segments.
    pub fn printBoxLineMulti(
        self: *TerminalUI,
        width: usize,
        segments: []const Segment,
    ) void {
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_V) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, " ") catch {};

        var text_len: usize = 0;
        for (segments) |seg| {
            self.writeColor(seg.color, seg.text);
            text_len += seg.text.len;
        }

        // Pad to width
        const content_width = width - 4;
        if (text_len < content_width) {
            for (0..content_width - text_len) |_| {
                self.stderr.writeStreamingAll(self.io, " ") catch {};
            }
        }
        self.stderr.writeStreamingAll(self.io, " ") catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_BORDER,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, BOX_V) catch {};
        if (self.is_tty) self.stderr.writeStreamingAll(
            self.io,
            ESC_RESET,
        ) catch {};
        self.stderr.writeStreamingAll(self.io, "\n") catch {};
    }
};

/// TablePrinter for ANSI box-drawn tables.
/// Prints tables with colored borders and headers.
pub const TablePrinter = struct {
    ui: *TerminalUI,
    col_widths: []const usize,

    /// Helper: set border color
    fn borderOn(self: *TablePrinter) void {
        if (self.ui.is_tty) self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.ESC_BORDER,
        ) catch {};
    }

    /// Helper: reset color
    fn colorOff(self: *TablePrinter) void {
        if (self.ui.is_tty) self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.ESC_RESET,
        ) catch {};
    }

    /// Print top border: ┌───────┬───────┬───────┐
    pub fn printTop(self: *TablePrinter) void {
        self.borderOn();
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_TL,
        ) catch {};
        for (self.col_widths, 0..) |w, i| {
            for (0..w) |_| {
                self.ui.stderr.writeStreamingAll(
                    self.ui.io,
                    TerminalUI.BOX_H,
                ) catch {};
            }
            if (i < self.col_widths.len - 1) {
                self.ui.stderr.writeStreamingAll(
                    self.ui.io,
                    TerminalUI.BOX_TT,
                ) catch {};
            }
        }
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_TR,
        ) catch {};
        self.colorOff();
        self.ui.stderr.writeStreamingAll(self.ui.io, "\n") catch {};
    }

    /// Print separator: ├───────┼───────┼───────┤
    pub fn printSeparator(self: *TablePrinter) void {
        self.borderOn();
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_LT,
        ) catch {};
        for (self.col_widths, 0..) |w, i| {
            for (0..w) |_| {
                self.ui.stderr.writeStreamingAll(
                    self.ui.io,
                    TerminalUI.BOX_H,
                ) catch {};
            }
            if (i < self.col_widths.len - 1) {
                self.ui.stderr.writeStreamingAll(
                    self.ui.io,
                    TerminalUI.BOX_X,
                ) catch {};
            }
        }
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_RT,
        ) catch {};
        self.colorOff();
        self.ui.stderr.writeStreamingAll(self.ui.io, "\n") catch {};
    }

    /// Print bottom border: └───────┴───────┴───────┘
    pub fn printBottom(self: *TablePrinter) void {
        self.borderOn();
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_BL,
        ) catch {};
        for (self.col_widths, 0..) |w, i| {
            for (0..w) |_| {
                self.ui.stderr.writeStreamingAll(
                    self.ui.io,
                    TerminalUI.BOX_H,
                ) catch {};
            }
            if (i < self.col_widths.len - 1) {
                self.ui.stderr.writeStreamingAll(
                    self.ui.io,
                    TerminalUI.BOX_BT,
                ) catch {};
            }
        }
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_BR,
        ) catch {};
        self.colorOff();
        self.ui.stderr.writeStreamingAll(self.ui.io, "\n") catch {};
    }

    /// Print header row with colored text.
    pub fn printHeaderRow(
        self: *TablePrinter,
        headers: []const []const u8,
    ) void {
        assert(headers.len == self.col_widths.len);

        self.borderOn();
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_V,
        ) catch {};
        self.colorOff();
        for (headers, 0..) |header, i| {
            self.ui.stderr.writeStreamingAll(self.ui.io, " ") catch {};
            self.ui.writeColor(TerminalUI.ESC_HEADER, header);

            // Pad to column width
            const pad = self.col_widths[i] - header.len - 1;
            for (0..pad) |_| {
                self.ui.stderr.writeStreamingAll(self.ui.io, " ") catch {};
            }
            self.borderOn();
            self.ui.stderr.writeStreamingAll(
                self.ui.io,
                TerminalUI.BOX_V,
            ) catch {};
            self.colorOff();
        }
        self.ui.stderr.writeStreamingAll(self.ui.io, "\n") catch {};
    }

    /// Print a data row.
    pub fn printRow(self: *TablePrinter, cells: []const []const u8) void {
        assert(cells.len == self.col_widths.len);

        self.borderOn();
        self.ui.stderr.writeStreamingAll(
            self.ui.io,
            TerminalUI.BOX_V,
        ) catch {};
        self.colorOff();
        for (cells, 0..) |cell, i| {
            self.ui.stderr.writeStreamingAll(self.ui.io, " ") catch {};
            self.ui.stderr.writeStreamingAll(self.ui.io, cell) catch {};

            // Pad to column width
            const cell_len = cell.len;
            const pad = if (self.col_widths[i] > cell_len + 1)
                self.col_widths[i] - cell_len - 1
            else
                0;
            for (0..pad) |_| {
                self.ui.stderr.writeStreamingAll(self.ui.io, " ") catch {};
            }
            self.borderOn();
            self.ui.stderr.writeStreamingAll(
                self.ui.io,
                TerminalUI.BOX_V,
            ) catch {};
            self.colorOff();
        }
        self.ui.stderr.writeStreamingAll(self.ui.io, "\n") catch {};
    }

    /// Print a data row with first cell colored (for client name).
    pub fn printRowHighlight(
        self: *TablePrinter,
        cells: []const []const u8,
    ) void {
        assert(cells.len == self.col_widths.len);

        self.borderOn();
        self.ui.stderr.writeStreamingAll(self.ui.io, TerminalUI.BOX_V) catch {};
        self.colorOff();
        for (cells, 0..) |cell, i| {
            self.ui.stderr.writeStreamingAll(self.ui.io, " ") catch {};
            if (i == 0) {
                self.ui.writeColor(TerminalUI.ESC_BOLD, cell);
            } else {
                self.ui.writeColor(TerminalUI.ESC_RATE, cell);
            }

            // Pad to column width
            const cell_len = cell.len;
            const pad = if (self.col_widths[i] > cell_len + 1)
                self.col_widths[i] - cell_len - 1
            else
                0;
            for (0..pad) |_| {
                self.ui.stderr.writeStreamingAll(self.ui.io, " ") catch {};
            }
            self.borderOn();
            self.ui.stderr.writeStreamingAll(
                self.ui.io,
                TerminalUI.BOX_V,
            ) catch {};
            self.colorOff();
        }
        self.ui.stderr.writeStreamingAll(self.ui.io, "\n") catch {};
    }
};

/// Buffered stdout writer for table output.
pub const StdOut = struct {
    buffer: [4096]u8 = undefined,
    file_writer: File.Writer = undefined,
    io: Io = undefined,

    /// Initialize buffered stdout.
    pub fn init(self: *StdOut, io: Io) void {
        self.io = io;
        self.file_writer = File.stdout().writer(io, &self.buffer);
    }

    /// Print formatted output.
    pub fn print(self: *StdOut, comptime fmt: []const u8, args: anytype) void {
        self.file_writer.interface.print(fmt, args) catch {};
    }

    /// Flush buffered output.
    pub fn flush(self: *StdOut) void {
        self.file_writer.interface.flush() catch {};
    }
};
