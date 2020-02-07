/**
 * 
 */
package com.zekelabs.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.zekelabs.kafka.pojo.SalesData;

/**
 * @author edyoda
 *
 */
public class ReadExcelFile {

	@SuppressWarnings("deprecation")
	public static List<SalesData> readFile(String xlsxfilename) {
		ArrayList<SalesData> salesList = new ArrayList<SalesData>();
		try {
			File file = new File(xlsxfilename);
			FileInputStream fis = new FileInputStream(file);
			XSSFWorkbook wb = new XSSFWorkbook(fis);
			XSSFSheet sheet = wb.getSheetAt(0);
			Iterator<Row> itr = sheet.iterator();
			int rownum = 0;
			while (itr.hasNext()) {
				SalesData sales = new SalesData();
				Row row = itr.next();
				Iterator<Cell> cellIterator = row.cellIterator();
				int cellnum = 0;
				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					if(rownum>1) {
						if(cellnum==0) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setInvoiceNumber(cell.getStringCellValue());
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setInvoiceNumber(cell.getNumericCellValue()+"");
							}
						} else if(cellnum==1) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setStockCode(cell.getStringCellValue());
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setStockCode(cell.getNumericCellValue() + "");
							}
						} else if(cellnum==2) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setDescription(cell.getStringCellValue());
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setDescription(cell.getNumericCellValue() + "");
							}
						} else if(cellnum==3) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setQuantity(Integer.parseInt(cell.getStringCellValue()));
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setQuantity((int)cell.getNumericCellValue());
							}
						} else if(cellnum==4) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setInvoiceDate(cell.getStringCellValue());
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setInvoiceDate(cell.getNumericCellValue() + "");
							}
						} else if(cellnum==5) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setUnitPrice(Double.parseDouble(cell.getStringCellValue()));
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setUnitPrice(cell.getNumericCellValue());
							}
						} else if(cellnum==6) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setCustomerID(cell.getStringCellValue());
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setCustomerID(cell.getNumericCellValue() + "");
							}
						} else if(cellnum==7) {
							if(cell.getCellTypeEnum() == CellType.STRING) {
								sales.setCountry(cell.getStringCellValue());
							} else if(cell.getCellTypeEnum() == CellType.NUMERIC) {
								sales.setCountry(cell.getNumericCellValue()+"");
							}
						}
					}
					
					/*
					 * if(cell.getCellTypeEnum() == CellType.STRING) {
					 * System.out.print(cell.getStringCellValue() + "\t"); } else
					 * if(cell.getCellTypeEnum() == CellType.NUMERIC) {
					 * System.out.print(cell.getNumericCellValue() + "\t"); } else {
					 * System.out.print("^"); }
					 */
					cellnum++;
				}
				//System.out.println("");
				if(rownum>1)
					salesList.add(sales);
				rownum++;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return salesList;
	}
	public static void main(String[] args) {
		List<SalesData> list = ReadExcelFile.readFile("/home/edyoda/Downloads/Kafka-java-projects/Project1/Sales.xlsx");
		SalesData data = list.get(0);
		System.out.println("Country:" + data.getCountry() + ",StockCode:" + data.getStockCode() + ",Quantity:" + data.getQuantity() + ",UnitePrice:" + data.getUnitPrice());
	}
}
