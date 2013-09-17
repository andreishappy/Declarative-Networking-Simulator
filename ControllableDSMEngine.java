package uk.ac.imperial.dsm.engines;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Scanner;

import com.ibm.watson.dsm.DSMException;
import com.ibm.watson.dsm.engine.EngineLogger;
import com.ibm.watson.dsm.engine.TupleState;
import com.ibm.watson.dsm.engine.app.DSMEngine;
import com.ibm.watson.dsm.platform.ApplicationDescriptor;
import com.ibm.watson.dsm.platform.IApplicationDescriptor;
import com.ibm.watson.dsm.platform.tuples.IColumnDescriptor;
import com.ibm.watson.dsm.platform.tuples.ITuple;
import com.ibm.watson.dsm.platform.tuples.ITupleSet;
import com.ibm.watson.dsm.platform.tuples.ITupleSetDescriptor;
import com.ibm.watson.dsm.platform.tuples.Tuple;
import com.ibm.watson.dsm.platform.tuples.TupleEntry;
import com.ibm.watson.dsm.platform.tuples.TupleEntryType;
import com.ibm.watson.dsm.platform.tuples.TupleSet;
import com.ibm.watson.pml.util.CommandArgs;

public class ControllableDSMEngine extends DSMEngine {
	private String initStateFile;
	private Socket controllerSocket;
	private Scanner controlIn;
	private PrintWriter controlOut;
	private int stateCount;

	public ControllableDSMEngine(IApplicationDescriptor appDesc,
			String dsmRuleFile, String host, int port) throws IOException,
			DSMException {
		this(appDesc, dsmRuleFile, host, port, null);
	}

	public ControllableDSMEngine(IApplicationDescriptor appDesc,
			String dsmRulesFile, String host, int port, String initStateFile)
			throws IOException, DSMException {
		super(appDesc, dsmRulesFile);
		this.initStateFile = initStateFile;

		connectToController(host, port);
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, DSMException {
		if (args.length == 0) {
			EngineLogger.logger
					.severe("Must provide application name, controller address and rules file");
		}

		CommandArgs cmdargs = new CommandArgs(args);
		if (cmdargs.getFlag("help")) {
			System.out.println(MainUsage);
			return;
		}

		String appName = cmdargs.getOption("app");
		if (appName == null)
			appName = cmdargs.getOption("namespace");
		if (appName == null) {
			EngineLogger.logger
					.severe("-app or -namespace argument must be provided to specify namespace for engine.");
			return;
		}
		String instance = cmdargs.getOption("instance");
		String controllerHost = cmdargs.getOption("controllerHost");
		int controllerPort = Integer.parseInt(cmdargs.getOption("controllerPort")); 
		String initFile = cmdargs.getOption("init");
		String file = args[(args.length - 1)];
		if (file == null) {
			EngineLogger.logger
					.severe("Rules file must be provided on the command line");
			System.out.println(MainUsage);
			return;
		}
		boolean interactive = cmdargs.getFlag("interactive");
		
		IApplicationDescriptor appDesc = new ApplicationDescriptor(appName,
				instance);
		DSMEngine engine = new ControllableDSMEngine(appDesc, file, controllerHost, controllerPort, initFile);
		engine.start();
		if (interactive) {
			System.out.print("Engine " + appDesc
					+ " started.  Press enter to terminate: ");
			System.in.read();
		} else {
			System.out.println("Engine " + appDesc + " started.");
			while (true)
				Thread.yield();
		}
		System.out.println("Engine " + appDesc + " exiting.");
		engine.stop();
	}

	private void connectToController(String host, int port)
			throws UnknownHostException, IOException {
		controllerSocket = new Socket(host, port);

		controlOut = new PrintWriter(controllerSocket.getOutputStream(), true);
		controlIn = new Scanner(controllerSocket.getInputStream());
	}

	private void disconnectFromController() throws IOException {
		controlIn.close();
		controlOut.close();
		controllerSocket.close();
	}

	public void stop() throws DSMException {
		super.stop();
		try {
			disconnectFromController();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void start() throws DSMException {
		super.start();

		if (initStateFile != null) {
			this.rwLockedTupleStorage.writeLock();
			try {

				insertInitialTuples();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			this.rwLockedTupleStorage.writeUnlock();
		}

		resetStateCount();

		// System.out.println("Interrupted " + getApplicationDescriptor()
		// + " on start.");
		interrupt();
		// System.out.println("Resumed " + getApplicationDescriptor()
		// + " on start.");
	}

	protected void evaluationComplete() {
		super.evaluationComplete();

		incrementStateCount();

		try {
			interrupt();
		} catch (DSMException e) {
			e.printStackTrace();
		}
	}

	protected void interrupt() throws DSMException {
		reportState();
		waitForCommand();
	}

	private void waitForCommand() {
		while (true) {
			String line = controlIn.nextLine();
			if (line.startsWith("RESUME")) {
				break;
			} else if (line.startsWith("CHANGE")) {
				receiveDeleteTuplesFromController();
				receiveAddTuplesFromController();
			}
		}
	}

	private void receiveAddTuplesFromController() {
		int numAddTuples = Integer.parseInt(controlIn.nextLine());

		while (numAddTuples-- > 0) {
			try {
				insertTuple(controlIn.nextLine());
			} catch (DSMException e) {
				e.printStackTrace();
			}
		}
	}

	private void receiveDeleteTuplesFromController() {
		int numDelTuples = Integer.parseInt(controlIn.nextLine());

		while (numDelTuples-- > 0) {
			try {
				removeTuple(controlIn.nextLine());
			} catch (DSMException e) {
				e.printStackTrace();
			}
		}
	}

	private void reportState() throws DSMException {
		controlOut.println(getApplicationDescriptor());
		controlOut.println(getStateCount() + " " + System.currentTimeMillis());

		TupleState state = getTupleState();

		reportTupleSets(state.getInputTuples());
		reportTupleSets(state.getPersistentTuples());
		reportTupleSets(state.getTransportTuples());
	}

	private void reportTupleSets(ITupleSet[] tSets) {
		int tCount = 0;
		StringBuffer sb = new StringBuffer();

		for (ITupleSet tSet : tSets) {
			String tName = tSet.getName();
			for (ITuple t : tSet) {
				tCount++;
				sb.append(tName + "(");
				for (int i = 0; i < t.size(); i++) {
					sb.append((i > 0 ? "," : "") + t.get(i).getValue());
				}
				sb.append(");\n");
			}
		}

		controlOut.print(tCount + "\n" + sb.toString());
		controlOut.flush();
	}

	private void insertInitialTuples() throws FileNotFoundException,
			DSMException {
		Timestamp ts = new Timestamp(System.currentTimeMillis());

		Scanner in = new Scanner(new File(initStateFile));
		while (in.hasNextLine()) {
			String line = in.nextLine().trim();

			insertTuple(ts, line);
		}
		in.close();
	}

	private void insertTuple(Timestamp ts, String tupleLine)
			throws DSMException {
		if (!tupleLine.isEmpty()) {
			String tname = tupleLine.substring(0, tupleLine.indexOf('('));
			String[] params = tupleLine.substring(tupleLine.indexOf('(') + 1,
					tupleLine.indexOf(')')).split(",");

			ITupleSetDescriptor tsDesc = this.rwLockedTupleStorage
					.getDescriptor(applicationDescriptor, tname);
			List<IColumnDescriptor> cDescList = tsDesc.getColumns();

			ITuple tuple = new Tuple();
			for (int i = 0; i < params.length; i++) {
				tuple.add(new TupleEntry(TupleEntryType.stringToObject(
						params[i], cDescList.get(i).getType())));
			}
			tuple.add(new TupleEntry(ts));

			this.rwLockedTupleStorage.append(applicationDescriptor,
					new TupleSet(tsDesc, tuple));
		}
	}

	private void insertTuple(String tupleLine) throws DSMException {
		if (!tupleLine.isEmpty()) {
			String tname = tupleLine.substring(0, tupleLine.indexOf('('));
			String[] params = tupleLine.substring(tupleLine.indexOf('(') + 1,
					tupleLine.indexOf(')')).split(",");

			ITupleSetDescriptor tsDesc = this.rwLockedTupleStorage
					.getDescriptor(applicationDescriptor, tname);
			List<IColumnDescriptor> cDescList = tsDesc.getColumns();

			ITuple tuple = new Tuple();
			for (int i = 0; i < params.length; i++) {
				tuple.add(new TupleEntry(TupleEntryType.stringToObject(
						params[i], cDescList.get(i).getType())));
			}

			this.rwLockedTupleStorage.append(applicationDescriptor,
					new TupleSet(tsDesc, tuple));
		}
	}

	private void removeTuple(String tupleLine) throws DSMException {
		if (!tupleLine.isEmpty()) {
			String tname = tupleLine.substring(0, tupleLine.indexOf('('));
			String[] params = tupleLine.substring(tupleLine.indexOf('(') + 1,
					tupleLine.indexOf(')')).split(",");

			ITupleSetDescriptor tsDesc = this.rwLockedTupleStorage
					.getDescriptor(applicationDescriptor, tname);
			List<IColumnDescriptor> cDescList = tsDesc.getColumns();

			ITuple tuple = new Tuple();
			for (int i = 0; i < params.length; i++) {
				tuple.add(new TupleEntry(TupleEntryType.stringToObject(
						params[i], cDescList.get(i).getType())));
			}

			this.rwLockedTupleStorage.delete(applicationDescriptor,
					new TupleSet(tsDesc, tuple));
		}
	}

	private void resetStateCount() {
		stateCount = 0;
	}

	private int getStateCount() {
		return stateCount;
	}

	private int incrementStateCount() {
		return ++stateCount;
	}

}
