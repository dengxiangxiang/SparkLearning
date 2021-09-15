package com.dxx.radarmap;

import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.io.AbstractGridFormat;
import org.geotools.coverage.grid.io.GridCoverage2DReader;
import org.geotools.coverage.grid.io.GridFormatFinder;
import org.geotools.coverage.processing.CoverageProcessor;
import org.geotools.gce.geotiff.GeoTiffFormat;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.util.factory.Hints;
import org.opengis.coverage.grid.GridCoverageWriter;
import org.opengis.geometry.Envelope;
import org.opengis.parameter.ParameterValueGroup;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;

public class TileContext {

    Logger log = LoggerFactory.getLogger(TileContext.class);

    AbstractGridFormat format;
    GridCoverage2D gridCoverage;
    CoordinateReferenceSystem coordinateReferenceSystem;

    private double coverageMinX;
    private double coverageMaxX;
    private double coverageMinY;
    private double coverageMaxY;

    public TileContext(File file) throws IOException {
        format = GridFormatFinder.findFormat(file);

        // working around a bug/quirk in geotiff loading via format.getReader which doesn't set this
        // correctly
        Hints hints = null;
        if (format instanceof GeoTiffFormat) {
            hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        }

        GridCoverage2DReader gridReader = format.getReader(file, hints);
        gridCoverage = gridReader.read(null);

        coordinateReferenceSystem = gridCoverage.getCoordinateReferenceSystem();
        log.info("CRS:" + coordinateReferenceSystem.getName());

        Envelope2D coverageEnvelope = gridCoverage.getEnvelope2D();
        log.info("dimension:" + coverageEnvelope.getDimension());

        coverageMinX = coverageEnvelope.getBounds().getMinX();
        coverageMaxX = coverageEnvelope.getBounds().getMaxX();
        coverageMinY = coverageEnvelope.getBounds().getMinY();
        coverageMaxY = coverageEnvelope.getBounds().getMaxY();

        log.info(String.format("minX:%f, maxX:%f, minY:%f, maxY:%f", coverageMinX, coverageMaxX, coverageMinY, coverageMaxY));
    }

    @Deprecated
    public TileContext(InputStream inputStream) throws IOException {


        format = GridFormatFinder.findFormat(inputStream);

        // working around a bug/quirk in geotiff loading via format.getReader which doesn't set this
        // correctly
        Hints hints = null;
        if (format instanceof GeoTiffFormat) {
            hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        }

        GridCoverage2DReader gridReader = format.getReader(inputStream, hints);
        gridCoverage = gridReader.read(null);

        coordinateReferenceSystem = gridCoverage.getCoordinateReferenceSystem();
        log.info("CRS:" + coordinateReferenceSystem.getName());

        Envelope2D coverageEnvelope = gridCoverage.getEnvelope2D();
        log.info("dimension:" + coverageEnvelope.getDimension());

        coverageMinX = coverageEnvelope.getBounds().getMinX();
        coverageMaxX = coverageEnvelope.getBounds().getMaxX();
        coverageMinY = coverageEnvelope.getBounds().getMinY();
        coverageMaxY = coverageEnvelope.getBounds().getMaxY();

        log.info(String.format("minX:%f, maxX:%f, minY:%f, maxY:%f", coverageMinX, coverageMaxX, coverageMinY, coverageMaxY));
    }

    private boolean checkValidRange0(double lon0, double lon1, double lat0, double lat1) {

        if (lon0 < coverageMinX ||
                lon1 > coverageMaxX ||
                lat0 < coverageMinY ||
                lat1 > coverageMaxY) {
            return false;
        } else {
            return true;
        }
    }

    private static boolean checkValidRange(double lon0, double lon1, double lat0, double lat1) {
//        minX:-131.000000, maxX:-50.000000, minY:5.000000, maxY:59.000000
        if (lon0 < -130 ||
                lon1 > -51 ||
                lat0 < 6 ||
                lat1 > 58) {
            return false;
        } else {
            return true;
        }
    }

    public static boolean checkValidZXY(int z, int x, int y) {
        double lon0 = MathUtil.tileXToLon(x, z);
        double lon1 = MathUtil.tileXToLon(x + 1, z);

        double lat1 = MathUtil.tileYToLat(y, z);
        double lat0 = MathUtil.tileYToLat(y + 1, z);

        return checkValidRange(lon0, lon1, lat0, lat1);

    }


    public boolean outputTif(int z, int x, int y, OutputStream outputStream) throws IOException {
        double lon0 = MathUtil.tileXToLon(x, z);
        double lon1 = MathUtil.tileXToLon(x + 1, z);

        double lat1 = MathUtil.tileYToLat(y, z);
        double lat0 = MathUtil.tileYToLat(y + 1, z);

        log.debug(String.format("lon0:%s, lon1:%s, lat0:%s, lat1:%s", lon0, lon1, lat0, lat1));

        if (!checkValidRange(lon0, lon1, lat0, lat1)) {
            return false;
        }

        ReferencedEnvelope referencedEnvelope = new ReferencedEnvelope(lon0, lon1, lat0, lat1, coordinateReferenceSystem);
        GridCoverage2D targetCoverage2D = cropCoverage(gridCoverage, referencedEnvelope);

        GridCoverageWriter writer = format.getWriter(outputStream);
        writer.write(targetCoverage2D, null);

        outputStream.flush();
        outputStream.close();

        return true;

    }

    private GridCoverage2D cropCoverage(GridCoverage2D gridCoverage, Envelope envelope) {
        CoverageProcessor processor = CoverageProcessor.getInstance();

        // An example of manually creating the operation and parameters we want
        final ParameterValueGroup param = processor.getOperation("CoverageCrop").getParameters();
        param.parameter("Source").setValue(gridCoverage);
        param.parameter("Envelope").setValue(envelope);

        return (GridCoverage2D) processor.doOperation(param);
    }


    public void outputPng(int z, int x, int y, OutputStream outputStream) throws IOException {

        long t0 = System.currentTimeMillis();

        double lon0 = MathUtil.tileXToLon(x, z);
        double lon1 = MathUtil.tileXToLon(x + 1, z);

        double lat1 = MathUtil.tileYToLat(y, z);
        double lat0 = MathUtil.tileYToLat(y + 1, z);

        log.debug(String.format("z:%s, x:%s, y:%s", z, x, y));
        log.debug(String.format("lon0:%s, lon1:%s, lat0:%s, lat1:%s", lon0, lon1, lat0, lat1));

        ReferencedEnvelope referencedEnvelope = new ReferencedEnvelope(lon0, lon1, lat0, lat1, coordinateReferenceSystem);
        GridCoverage2D targetCoverage2D = cropCoverage(gridCoverage, referencedEnvelope);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GridCoverageWriter writer = format.getWriter(byteArrayOutputStream);
        writer.write(targetCoverage2D, null);
        byteArrayOutputStream.flush();


        // convert to png
        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        BufferedImage bufferedImage = ImageIO.read(inputStream);

        int type = bufferedImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB : bufferedImage.getType();
        int width = bufferedImage.getWidth();
        int height = bufferedImage.getHeight();

        BufferedImage resizedImage = new BufferedImage(width, height, type);

        Graphics2D graphics2d = resizedImage.createGraphics();
        graphics2d.drawImage(bufferedImage, 0, 0, width, height, null);//resize goes here
        graphics2d.dispose();

        ImageIO.write(resizedImage, "png", outputStream);

        outputStream.flush();
        outputStream.close();

        long dt = System.currentTimeMillis() - t0;
        log.debug("time costs:" + dt);

        log.debug(String.format("z:%s, x:%s, y:%s done!", z, x, y));

    }

//    @Deprecated
//    public void outputPngInStandardSize(int z, int x, int y, OutputStream outputStream) throws IOException {
//
//        double lon0 = MathUtil.tileXToLon(x, z);
//        double lon1 = MathUtil.tileXToLon(x + 1, z);
//
//        double lat1 = MathUtil.tileYToLat(y, z);
//        double lat0 = MathUtil.tileYToLat(y + 1, z);
//
//        System.out.println(String.format("lon0:%s, lon1:%s, lat0:%s, lat1:%s", lon0, lon1, lat0, lat1));
//
//        ReferencedEnvelope referencedEnvelope = new ReferencedEnvelope(lon0, lon1, lat0, lat1, coordinateReferenceSystem);
//        GridCoverage2D targetCoverage2D = cropCoverage(gridCoverage, referencedEnvelope);
//
//
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        GridCoverageWriter writer = format.getWriter(byteArrayOutputStream);
//        writer.write(targetCoverage2D, null);
//        byteArrayOutputStream.flush();
//
//
//        ByteArrayInputStream inputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
//
//
//        BufferedImage bufferedImage = ImageIO.read(inputStream);
//
//
//        int type = bufferedImage.getType() == 0 ? BufferedImage.TYPE_INT_ARGB : bufferedImage.getType();
//        int height = bufferedImage.getHeight();
//        int width = bufferedImage.getWidth();
//        System.out.println(String.format("origin height:%s, origin width: %s", height, width));
//
//        BufferedImage resizedImage = new BufferedImage(256, 256, type);
//
//        Graphics2D graphics2d = resizedImage.createGraphics();
//        graphics2d.drawImage(bufferedImage, 0, 0, 256, 256, null);//resize goes here
//        graphics2d.dispose();
//
//        ImageIO.write(resizedImage, "png", outputStream);
//
//        outputStream.flush();
//        outputStream.close();
//
//    }


//    /**
//     * Scale the coverage based on the set tileScale
//     *
//     * <p>As an alternative to using parameters to do the operations, we can use the Operations
//     * class to do them in a slightly more type safe way.
//     *
//     * @param coverage the coverage to scale
//     * @return the scaled coverage
//     */
//    @Deprecated
//    private GridCoverage2D scaleCoverage(GridCoverage2D coverage) {
//        Operations ops = new Operations(null);
//        coverage =
//                (GridCoverage2D)
//                        ops.scale(coverage, this.getTileScale(), this.getTileScale(), 0, 0);
//        return coverage;
//    }

    public static byte[] getByteArray(InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }


}
