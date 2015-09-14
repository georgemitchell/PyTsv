import codecs
import csv


def unicode_csv_reader(utf8_data, dialect=csv.excel, **kwargs):
    csv_reader = csv.reader(utf8_data, dialect=dialect, **kwargs)
    for row in csv_reader:
        yield [unicode(cell, 'utf-8') for cell in row]


class TsvReader:
    def __init__(self,
                 filename,
                 includes_header=True,
                 header=None,
                 header_values=None,
                 guess_at_values=False,
                 encoding=None,
                 delimiter="\t"):
        self.delimiter = delimiter
        if encoding == "utf-8":
            self.file_ptr = codecs.open(filename, "r", "utf-8")
            self.reader = unicode_csv_reader(self.file_ptr,
                                             delimiter=delimiter)
        else:
            self.file_ptr = open(filename, "rb")
            self.reader = csv.reader(self.file_ptr, delimiter=delimiter)

        # first row of the file contains the header
        if includes_header:
            if header is None:
                self.header = self.reader.next()
            else:
                # the passed in header value supercedes,
                # so just skip the first row
                self.reader.next()
        else:
            if header is None:
                self.header = None
            else:
                self.header = header

        if header_values is None:
            if guess_at_values:
                self.header_values = []
                first_row = self.reader.next()
                for val in first_row:
                    try:
                        float(val)
                        if val.find(".") == -1:
                            self.header_values.append(int)
                        else:
                            self.header_values.append(float)
                    except ValueError:
                        try:
                            s = unicode(val)
                            self.header_values.append(unicode)
                        except:
                            self.header_values.append(None)
                # reset the counter
                self.file_ptr.seek(0)
                if includes_header:
                    self.file_ptr.next()
            else:
                self.header_values = None
        else:
            self.header_values = header_values

    def rows(self):
        for row in self.reader:
            if self.header is not None:
                output = {}
                for i, val in enumerate(row):
                    output[self.header[i]] = self.convert_val(val, i)
                yield output
            else:
                if self.header_values is not None:
                    yield [self.convert_val(val, i) for i, val in enumerate(row)]
                else:
                    yield row

    def convert_val(self, val, index):
        try:
            return self.header_values[index](val)
        except ValueError:
            return val
        except TypeError:
            return val

    def close(self):
        self.file_ptr.close()
