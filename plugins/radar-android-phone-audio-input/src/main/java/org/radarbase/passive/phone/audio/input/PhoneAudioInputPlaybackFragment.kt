package org.radarbase.passive.phone.audio.input

import android.content.Context
import android.media.MediaPlayer
import android.net.Uri
import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.fragment.app.Fragment
import org.radarbase.passive.phone.audio.input.PhoneAudioInputActivity.Companion.AUDIO_FILE_NAME
import org.radarbase.passive.phone.audio.input.databinding.FragmentAudioInputPlaybackBinding
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException

class PhoneAudioInputPlaybackFragment : Fragment() {

    private var binding: FragmentAudioInputPlaybackBinding? = null
    private var mediaPlayer: MediaPlayer? = null

    private var audioFilePath: String? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val args: Bundle = checkNotNull(arguments) { "Cannot Start Playback without the recorded file location" }
        audioFilePath = args.getString(AUDIO_FILE_NAME)

//        mediaPlayer = MediaPlayer()
//        val audio = File(audioFilePath!!)
//        logger.info("Can Read Audio file: ${audio.canRead()}")
//        mediaPlayer = MediaPlayer.create(context, Uri.fromFile(audio))
//        mediaPlayer?.start()
//        mediaPlayer?.apply {
//            setDataSource(audioFilePath)
//            prepare()
//        }


        val audio = File(audioFilePath!!)
        val length = audio.length()
        if (audio.canRead()) {
            logger.info("Can Read Audio file: ${audio.canRead()}")
            mediaPlayer = MediaPlayer().apply {
                try {
                    setDataSource(requireContext(), Uri.fromFile(audio))
                    prepareAsync()
                    setOnPreparedListener {
                        start()
                    }
                    setOnErrorListener { mp, what, extra ->
                        logger.error("MediaPlayer Error: what=$what, extra=$extra")
                        true
                    }
                } catch (e: IOException) {
                    logger.error("IOException: ${e.message}")
                } catch (e: IllegalArgumentException) {
                    logger.error("IllegalArgumentException: ${e.message}")
                } catch (e: SecurityException) {
                    logger.error("SecurityException: ${e.message}")
                } catch (e: IllegalStateException) {
                    logger.error("IllegalStateException: ${e.message}")
                }
            }
        } else {
            logger.error("Cannot read audio file")
        }

    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?,
    ): View = FragmentAudioInputPlaybackBinding.inflate(inflater, container, false)
        .also { binding = it }
        .root

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)

        binding?.apply {
            startPlayback.setOnClickListener {
                mediaPlayer?.start()
            }
        }
    }

    override fun onDestroyView() {
        super.onDestroyView()
        binding = null
    }

    override fun onDestroy() {
        super.onDestroy()
        mediaPlayer?.release()
        mediaPlayer = null
    }


    interface OnPhoneAudioFragmentInteractionListener {
        fun onSendAudio()
        fun discardLatestRecording()
    }

    companion object {
        private val logger: Logger =
            LoggerFactory.getLogger(PhoneAudioInputPlaybackFragment::class.java)

        fun newInstance(context: Context, audioFileName: String): PhoneAudioInputPlaybackFragment =
            PhoneAudioInputPlaybackFragment().apply {
                arguments = Bundle().apply {
                    putString(AUDIO_FILE_NAME, audioFileName)
                }
            }
    }
}