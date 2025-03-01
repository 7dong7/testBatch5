package org.mybatch5.testbatch.batch;


import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

public class ExcelRowReader implements ItemStreamReader<Row> {

    /**
     *  filePath
     *      - excel 파일의 경로
     *      - 생성자에서 받는다
     *
     *  fileInputStream
     *      - 지정된 파일을 읽기위한 스트림 객체 (Apache POI 의 XSSWorkbook 사용)
     *
     *  workbook
     *      - excel 파일 전체를 나타내는 객체
     *
     *  rowCursor
     *      - excel 시트의 행(row)를 순회하기 위한 iterator
     *      - 기본적으로 엑셀의 첫 번째 시트의 모든 행을 순회한다
     *
     *  currentRowNumber
     *      - 현재 읽고 있는 행 번호. 
     *      - 초기값 1로 시작할 경우 헤더를 스킵할 수 있다. 0으로 시작하는 경우 맨 위에서 부터 읽는다
     *      - 이 값을 기록해 두면 체크포인트로 사용해 문제가 발생하면 처음부터 읽지 않고, currentRowNumber 에 해당하는 행 부터 읽을 수 있다
     *
     *  CURRENT_ROW_KEY
     *      - ExecutionContext 에 저장할 때 사용할 키 이름
     *      - 이 키를 통해 현재 행 번호(체크 포인트)를 저장하고 조회할 수 있다
     */
    private final String filePath;              // 엑셀 파일의 경로
    private FileInputStream fileInputStream;    // 해당 경로의 파일을 InputStream 으로 열기위한 객체
    private Workbook workbook;                  // 엑셀 파일을 열고 직접 받을 객체
    private Iterator<Row> rowCursor;            // 액셀의 각각의 행을 반복할 객체
    private int currentRowNumber;               // 어떤 행까지 반복을 실행했는지 기록하는 행번호
    private final String CURRENT_ROW_KEY = "current.row.number"; // 메타데이터 테이블에 기록한 값

    public ExcelRowReader(String filePath) throws IOException {
        this.filePath = filePath;
        this.currentRowNumber = 0; // 엑셀을 어디서 부터 읽어올지 정의 0행부터 읽어온다
    }

    /**
     *  단한번만 실행된다
     *  엑셀파일을 열거나 어떤 초기화를 진행하는데 사용된다
     */
    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {

        try {
            // 파일을 열어 workbook 객체(excel 파일)를 생성
            fileInputStream = new FileInputStream(filePath);
            workbook = WorkbookFactory.create(fileInputStream);
            Sheet sheet = workbook.getSheetAt(0);
            // 순회할 수 있도록 iterator 를 초기화
            this.rowCursor = sheet.iterator();

            // 동일 배치 파라미터에 대해 특정 키 값 "current.row.number"의 값이 존재한다면 초기화
            if (executionContext.containsKey(CURRENT_ROW_KEY)) {
                currentRowNumber = executionContext.getInt(CURRENT_ROW_KEY);
            }

            // 위의 값을 가져와 이미 실행한 부분은 건너 뜀
            for (int i = 0; i < currentRowNumber && rowCursor.hasNext(); i++) {
                rowCursor.next();
            }

        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    /**
     * 한번 열리고난다음
     * 청크 단위로 처리가 진행될때 매번 불려지는 메소드
     * 데이터의 각각의 행을 읽는다
     */
    @Override
    public Row read() {
        // rowCursor 에 대한 정보가 있는 경우, currentRowNumber 의 값을 증가시키고 다음 행을 반환한다
        if (rowCursor != null && rowCursor.hasNext()) {
            currentRowNumber++;
            return rowCursor.next();
        } else {
            return null;
        }
    }

    /**
     *  read가 불려지고 난 다음에 어떤값이 update가 일어나면 사용
     */
    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        // 체크 포인트 저장
            // 현재까지 처리한 행 번호를 ExecutionContext 에 저장
            // 복원시에 사용
        executionContext.putInt(CURRENT_ROW_KEY, currentRowNumber);
    }

    /**
     *  배치작업이 끝나면 사용된다
     *  열려있는 엑셀파일을 닫는다
     *  변수값을 다시 초기화한다
     */
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
