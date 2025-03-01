package org.mybatch5.testbatch.batch;

import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.mybatch5.testbatch.entity.WinEntity;
import org.springframework.batch.item.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

@Slf4j
public class ExcelReader implements ItemStreamReader<WinEntity> {

    private final String filePath;           // excel 파일 경로
    private FileInputStream fileInputStream; // 엑셀 파일을 열기 위한 InputStream
    private Workbook workbook;              // 엑셀 파일을 열고 엑셀파일 전체를 저장할 객체


    private Sheet sheet; // Iterator 대신 Sheet 직접 사용
    private Iterator<Row> rowCursor;            // 액셀의 각각의 행을 반복할 객체
    private int currentRowNum;          // 읽을 행 인덱스 번호: 0인 경우 처음부터, 1인 경우 헤더 스킵
    private final String CURRENT_ROW_KEY = "readExcel.current.row.number"; // 메타데이터 테이블에 기록한 값

    // ======= 생성자 ======= //
    public ExcelReader(String filePath) throws IOException {
        this.filePath = filePath;
        this.currentRowNum = 1;
    }

    // ======= 열기 ======= //
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            fileInputStream = new FileInputStream(filePath);
            workbook = WorkbookFactory.create(fileInputStream);
            sheet = workbook.getSheetAt(0);

            if (sheet == null) {
                throw new ItemStreamException("No sheets found in Excel file: " + filePath);
            }

            this.rowCursor = sheet.iterator();
            if (!rowCursor.hasNext()) {
                log.warn("Excel sheet is empty: " + filePath);
            } else {
                log.info("Excel sheet initialized with row count: {}", sheet.getLastRowNum() + 1);
            }
            
            if (executionContext.containsKey(CURRENT_ROW_KEY)) { // 기존에 읽던 곳이 있는 경우
                log.info("Resuming from row: {}", currentRowNum);
                this.currentRowNum = executionContext.getInt(CURRENT_ROW_KEY);
                // 위의 값을 가져와 이미 실행한 부분은 건너 뜀
                for (int i = 0; i < currentRowNum && rowCursor.hasNext(); i++) {
                    rowCursor.next();
                }
            }

        } catch (Exception e) {
            throw new ItemStreamException(e);
        }
    }


    // ======= 읽기 ======= //
    @Override
    public WinEntity read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (sheet == null) {
            log.warn("Sheet is null, cannot read rows");
            return null;
        }

        // 마지막 행까지 순회
        while (currentRowNum <= sheet.getLastRowNum()) {
            Row row = sheet.getRow(currentRowNum);
            currentRowNum++;

            // 빈 행 체크
            if (row == null || isRowEmpty(row)) {
                log.info("Skipping empty row at index: {}", currentRowNum - 1);
                continue;
            }

            // 셀 값 읽기
            String username = row.getCell(1) != null && row.getCell(1).getCellType() == CellType.STRING
                    ? row.getCell(1).getStringCellValue() : null;
            Long win = row.getCell(2) != null && row.getCell(2).getCellType() == CellType.NUMERIC
                    ? (long) row.getCell(2).getNumericCellValue() : null;
            Boolean reward = row.getCell(3) != null && row.getCell(3).getCellType() == CellType.BOOLEAN
                    ? row.getCell(3).getBooleanCellValue() : null;

            // 필수 값이 없으면 건너뛰기
            if (username == null || win == null || reward == null) {
                log.info("Skipping row with missing data at index: {}", currentRowNum - 1);
                continue;
            }

            WinEntity entity = WinEntity.builder()
                    .username(username)
                    .win(win)
                    .reward(reward)
                    .build();
            log.info("Read entity at row {}: {}", currentRowNum - 1, entity);
            return entity;
        }

        log.info("Reached end of sheet at row: {}", currentRowNum);
        return null;
//        if (rowCursor == null) {
//            log.warn("rowCursor is null, cannot read rows");
//            return null;
//        }
//        if (!rowCursor.hasNext()) {
//            log.info("No more rows to read at row number: {}", currentRowNum);
//            return null;
//        }
//
//        currentRowNum++;
//        Row row = rowCursor.next();
//
//        String username = row.getCell(1) != null && row.getCell(1).getCellType() == CellType.STRING ? row.getCell(1).getStringCellValue() : "";
//        long win = row.getCell(2) != null && row.getCell(2).getCellType() == CellType.NUMERIC ? (long) row.getCell(2).getNumericCellValue() : 0L;
//        boolean reward = row.getCell(3) != null && row.getCell(3).getCellType() == CellType.BOOLEAN ? row.getCell(3).getBooleanCellValue() : false;
//
//        WinEntity entity = WinEntity.builder()
//                .username(username)
//                .win(win)
//                .reward(reward)
//                .build();
//
//        log.info("WinEntity entity= {}", entity);
//        return entity;
    }

    // 빈 행 체크 헬퍼 메서드
    private boolean isRowEmpty(Row row) {
        if (row == null) return true;
        for (int i = row.getFirstCellNum(); i < row.getLastCellNum(); i++) {
            Cell cell = row.getCell(i);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                return false;
            }
        }
        return true;
    }


    // ======= 진행상황 저장 ======= //
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putInt(CURRENT_ROW_KEY, currentRowNum);
    }

    // ======= 닫기 ======= //
    @Override
    public void close() throws ItemStreamException {
        try {
            if (workbook != null) {
                // 자원 해 (작업이 종료된 workbook(액셀) 을 닫아 리소스를 해제한다
                workbook.close(); // 이미 읽어들인 엑셀 객체의 리소스를 해제 이후에 더 이상 참조하지 않을 경우 GC가 알아서 없앰
            }
            if (fileInputStream != null) {
                fileInputStream.close(); // 파일을 읽는 inputStream 을 해제한다
            }
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }
}
