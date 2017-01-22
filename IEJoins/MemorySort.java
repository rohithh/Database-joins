package IEJoins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import global.TupleOrder;
import heap.Tuple;
import iterator.FileScan;

public class MemorySort {

	FileScan am = null;
	int columnToSort = 0;
	int projColumn = 0;
	TupleOrder order;

	MemorySort(FileScan am, int columnToSort, TupleOrder order, int projCol) {
		this.am = am;
		this.columnToSort = columnToSort;
		this.order = order;
		this.projColumn = projCol;
	}

	public List<SortObject> sort() {
		Tuple t = new Tuple();
		int index = 0;
		List<SortObject> list = new ArrayList<SortObject>();
		try {
			while ((t = this.am.get_next()) != null) {

				SortObject obj = new SortObject();
				obj.setRowIndex(index);
				obj.setSortValue(t.getIntFld(this.columnToSort));
				obj.setProjColValue(t.getIntFld(this.projColumn));
				list.add(obj);
				index++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		Collections.sort(list, new Comparator<SortObject>() {

			public int compare(SortObject o1, SortObject o2) {

				if (order.tupleOrder == TupleOrder.Ascending) {
					return o1.getSortValue() < o2.getSortValue() ? -1 : o1.getSortValue() > o2.getSortValue() ? 1 : 0;
				} else {
					return o1.getSortValue() > o2.getSortValue() ? -1 : o1.getSortValue() < o2.getSortValue() ? 1 : 0;
				}

			}
		});

		return list;
	}

}
