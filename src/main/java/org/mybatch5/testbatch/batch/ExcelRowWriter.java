package org.mybatch5.testbatch.batch;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.mybatch5.testbatch.entity.BeforeEntity;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.FileOutputStream;
import java.io.IOException;

public class ExcelRowWriter implements ItemStreamWriter<BeforeEntity> {

    private final String filePath;
    private Workbook workbook;
    private Sheet sheet;
    private int currentRowNumber;
    private boolean isClosed;

    public ExcelRowWriter(String filePath) throws IOException {

        this.filePath = filePath;
        this.isClosed = false;
        this.currentRowNumber = 0;
    }

    /**
     *  배치 작업이 사작되기 전에 한 번 호출되어 Excel 파일의 기록 준비를 한다
     *
     *  workbook = new XSSFWorkbook();
     *      - 새 excel 워크북 객체를 생성한다 (새로운 액셀)
     *
     *  sheet = workbook.createSheet("Sheet1");
     *      - 워크북 내에 "Sheet1" 이라는 이름의 시트를 생성
     *      
     *  이 결과는 Excel 파일에 데이터를 기록하기위한 준비 단계이다
     *  이 후에, write() 메소드에서 데이터를 추가할 수 있다
     */
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        workbook = new XSSFWorkbook();
        sheet = workbook.createSheet("Sheet1");
    }


    /**
     *  배치 작업에서 청크 단위로 전달받은 BeforeEntity 객체들을 Excel 시트에 기록한다
     *  전달받은 각 청크(리스트 형태)의 각 BeforeEntity 객체데 대해 반복문 실행
     *
     *  Row row = sheet.createRow(currentRowNumber++);
     *      - 현재 row 번호에 해당하는 행을 생성하고, currentRowNumber 의 값을 하나 올린다
     *
     *  row.createCell(0).setCellValue(entity.getUsername());
     *      - 생성된 행의 첫 번째 셀(인덱스 0)에 entity의 username 값을 기록한다
     *      
     *  위 의 작업 반복적으로 수행
     */
    @Override
    public void write(Chunk<? extends BeforeEntity> chunk) {
        for (BeforeEntity entity : chunk) {
            Row row = sheet.createRow(currentRowNumber++);
            row.createCell(0).setCellValue(entity.getUsername());
        }
    }


    /**
     *  배치 작업이 모두 완료된 후 호출, 생성된 excel 워크북을 파일로 저장하고, 사용한 자원을 해제한다
     *
     *  if (isClosed) { return; }
     *      - 이미 close()가 호출되어 자원이 해제된 경우에는 추가 작업을 건너 뛴다
     *
     *  try (FileOutputStream fileOut = new FileOutputStream(filePath)) { workbook.write(fileOut); }
     *      - 파일 출력 스트림을 생성하여 filePath에 workbook의 내용을 기록한다
     *      - try 문을 사용해 자동으로 스트림이 닫히게 한다
     *      
     *  workbook.close();
     *      - workbook 자원을 해제한다
     *
     *  isClosed = true;
     *      - 작업이 완료되었음을 표시한다
     */
    @Override
    public void close() throws ItemStreamException {

        if (isClosed) {
            return;
        }

        try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
            workbook.write(fileOut);
        } catch (IOException e) {
            throw new ItemStreamException(e);
        } finally {
            try {
                workbook.close();
            } catch (IOException e) {
                throw new ItemStreamException(e);
            } finally {
                isClosed = true;
            }
        }
    }
}
