from pychart import *

theme.default_font_size = 11

class MyBarP(bar_plot.T):
	def get_legend_entry(self):
		ret = None
		if self.label:
			ret = legend.Entry(line_style=(self.legend_line_style or self.line_style),
			                   fill_style=(self.legend_fill_style or self.fill_style),
						   label=self.label)
		ret.rect_size = 10
		return ret


class MyCoord(category_coord.T):
    def get_canvas_pos(self, size, val, min, max):
        i = 0.0
        for v in self.data:
            if v[self.col] == val:
				ret = i * (size / (float(len(self.data) -.9)))

				return ret
            i += 1
        # the drawing area is clipped. So negative offset will make this plot
        # invisible.
        return canvas.invalid_coord;

class MyLineP(line_plot.T):
	def get_legend_entry(self):
		ret = line_plot.T.get_legend_entry(self)
		ret.line_len = 30
		return ret;
