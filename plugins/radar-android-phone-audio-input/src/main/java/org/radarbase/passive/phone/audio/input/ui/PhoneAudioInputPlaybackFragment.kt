/*
 * Copyright 2017 The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.passive.phone.audio.input.ui

import android.content.DialogInterface
import android.content.Intent
import android.media.AudioDeviceInfo
import android.media.MediaPlayer
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.view.WindowManager
import android.widget.Button
import android.widget.SeekBar
import android.widget.SeekBar.OnSeekBarChangeListener
import android.widget.Toast
import androidx.activity.OnBackPressedCallback
import androidx.fragment.app.Fragment
import androidx.fragment.app.activityViewModels
import org.radarbase.android.util.Boast
import org.radarbase.android.util.CoroutineTaskExecutor
import org.radarbase.passive.phone.audio.input.PhoneAudioInputState
import org.radarbase.passive.phone.audio.input.R
import org.radarbase.passive.phone.audio.input.databinding.FragmentAudioInputPlaybackBinding
import org.radarbase.passive.phone.audio.input.ui.PhoneAudioInputActivity.Companion.AUDIO_FILE_NAME
import org.radarbase.passive.phone.audio.input.ui.PhoneAudioInputActivity.Companion.EXTERNAL_DEVICE_NAME
import org.radarbase.passive.phone.audio.input.utils.AudioDeviceUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException


class PhoneAudioInputPlaybackFragment : Fragment() {

    private var binding: FragmentAudioInputPlaybackBinding? = null
    private lateinit var playbackButtons: List<Button>

    private var mediaPlayer: MediaPlayer? = null
    private val phoneAudioViewModel: PhoneAudioInputViewModel by activityViewModels<PhoneAudioInputViewModel>()
    private var phoneAudioInputState: PhoneAudioInputState? = null
    private val mediaPlaybackHandler: CoroutineTaskExecutor = CoroutineTaskExecutor(this::class.simpleName!!)
    private val mainHandler: Handler = Handler(Looper.getMainLooper())

    private var audioFilePath: String? = null

    private val errorListener = { mp: MediaPlayer, what: Int, extra: Int ->
        logger.error("MediaPlayer Error: what=$what, extra=$extra")
        mp.reset()
        true
    }

    private val preparedListener: (MediaPlayer) -> Unit = { mp: MediaPlayer ->
        mediaPlaybackHandler.execute(mp::start)
        postStartView()
        updateSeekbar()
        binding?.tvAudioDuration?.text = AudioDeviceUtils.formatMsToReadableTime(mp.duration.toLong())
        keepScreenOn()
        phoneAudioInputState?.isRecordingPlayed = true
    }

    private val completionListener: (MediaPlayer) -> Unit = { _: MediaPlayer ->
        preStartView()
        disableKeepScreenOn()
        binding?.apply {
            seekBar.progress = 0
            tvCurrentPosition.text = context?.getString(R.string.timer_text) ?: "00:00:00"
        }
    }

    private val seekBarRunnable: Runnable = object : Runnable {
        override fun run() {
            binding?.let {
                mediaPlayer?.let { mp ->
                    if (mp.isPlaying) {
                        it.seekBar.progress = mp.currentPosition
                        it.tvCurrentPosition.text = AudioDeviceUtils.formatMsToReadableTime(mp.currentPosition.toLong())
                        mainHandler.postDelayed(this, 100)
                    } else {
                        mainHandler.removeCallbacks(this)
                    }
                }
            }
        }
    }

    private fun preStartView(): Unit = setVisibilityFor(binding!!.btnStart)
    private fun postStartView(): Unit = setVisibilityFor(binding!!.btnPause)
    private fun postPauseView(): Unit = setVisibilityFor(binding!!.btnResume)

    private fun keepScreenOn() {
        requireActivity().window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
    }
    private fun disableKeepScreenOn() {
        requireActivity().window.clearFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
    }

    private fun setVisibilityFor(btn: Button?) {
        playbackButtons.forEach { button: Button ->
            if (button === btn) {
                button.visibility = View.VISIBLE
                button.isEnabled  = true
            } else {
                button.visibility = View.INVISIBLE
                button.isEnabled = false
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val args: Bundle = checkNotNull(arguments) { "Cannot Start Playback without the recorded file location" }
        audioFilePath = args.getString(AUDIO_FILE_NAME)
        mediaPlayer = MediaPlayer().apply {
            setOnPreparedListener(preparedListener)
            setOnErrorListener(errorListener)
            setOnCompletionListener(completionListener)
        }
        mediaPlaybackHandler.start()
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

        requireActivity().onBackPressedDispatcher.addCallback(viewLifecycleOwner, object: OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                AudioDeviceUtils.showAlertDialog(requireContext()) {
                    setTitle(getString(R.string.exit_confirmation))
                        .setMessage(getString(R.string.exit_message))
                        .setPositiveButton(getString(R.string.dialog_yes)) { dialog: DialogInterface, _ ->
                            phoneAudioInputState?.isRecordingPlayed = false
                            val intent = Intent(requireContext(), PhoneAudioInputActivity::class.java)

                            forwardMicIfPreferred(intent)

                            dialog.cancel()
                            requireActivity().finish()
                            startActivity(intent)
                            logger.info("Activity Restarted")
                        }
                        .setNegativeButton(getString(R.string.dialog_no)) { dialog: DialogInterface, _ ->
                        dialog.dismiss()
                    }
                }
            }
        })

        observeState()
        audioFilePath ?: run {
            Boast.makeText(requireActivity(), getString(R.string.cannot_start_player), Toast.LENGTH_SHORT).show(true)
            return
        }
        playbackButtons = listOf(binding!!.btnStart, binding!!.btnPause, binding!!.btnResume)
        setVisibilityFor(binding!!.btnStart)

        applyBinding(binding!!, File(audioFilePath!!))
    }

    private fun applyBinding(binding: FragmentAudioInputPlaybackBinding?, audio: File) {
        binding?.apply {

            if (audio.canRead()) {
                btnStart.setOnClickListener {
                    mediaPlayer?.apply {
                        if (isPlaying) return@setOnClickListener
                        reset()
                        try {
                            setDataSource(audioFilePath)
                            mediaPlaybackHandler.execute {
                                prepareAsync()
                            }
                        } catch (e: IOException) {
                            logger.error("IOException: ${e.message}")
                        } catch (e: IllegalArgumentException) {
                            logger.error("IllegalArgumentException: ${e.message}")
                        } catch (e: SecurityException) {
                            logger.error("SecurityException: ${e.message}")
                        } catch (e: IllegalStateException) {
                            logger.error("IllegalStateException: ${e.message}")
                        } catch (ex: Exception) {
                            logger.error("Exception while playing audio file: {}", ex.message)
                        }
                    }
                }
            } else {
                logger.error("Cannot read audio file")
            }

            btnPause.setOnClickListener {
                try {
                    mediaPlayer?.pause()
                    disableKeepScreenOn()
                    mainHandler.removeCallbacks(seekBarRunnable)
                } catch (ex: IllegalStateException) {
                 logger.error("Cannot pause, the internal player has not been initialized yet.")
                }
                    postPauseView()
                }

            btnResume.setOnClickListener {
                try {
                    mediaPlayer?.start()
                    keepScreenOn()
                    updateSeekbar()
                } catch (ex: Exception) {
                    logger.error("Cannot start playback, MediaPlayer is in invalid state.")
                }
                    postStartView()
                }

            seekBar.setOnSeekBarChangeListener(object : OnSeekBarChangeListener {
                override fun onProgressChanged(
                    seekBar: SeekBar?,
                    progress: Int,
                    fromUser: Boolean,
                ) {
                    if (fromUser) {
                        mediaPlayer?.seekTo(progress)
                    }
                }
                override fun onStartTrackingTouch(seekBar: SeekBar?) = Unit
                override fun onStopTrackingTouch(seekBar: SeekBar?) = Unit
            })

            btnSend.setOnClickListener {
                phoneAudioViewModel.phoneAudioState.value?.audioRecordingManager?.send()

                val intent = Intent(requireContext(), PhoneAudioInputActivity::class.java)
                forwardMicIfPreferred(intent)
                requireActivity().finish()
                startActivity(intent)
            }
        }
    }

    private fun forwardMicIfPreferred(intent: Intent) {
        (phoneAudioInputState?.finalizedMicrophone?.value)?.let {
            logger.debug("Forwarding preferred device: {}", it)
            val isMicPreferred = phoneAudioInputState?.microphonePrioritized ?: false
            if (it.type == AudioDeviceInfo.TYPE_BUILTIN_MIC && !isMicPreferred) return@let null
            it.productName.toString()
        }?.also {
            intent.putExtra(EXTERNAL_DEVICE_NAME, it)
        }
    }

    private fun updateSeekbar() {
        binding?.let {
            mediaPlayer?.let {  mp ->
                it.seekBar.max = mp.duration
                mainHandler.post(seekBarRunnable)
            }
        }
    }

    private fun observeState() {
        phoneAudioViewModel.phoneAudioState.observe(viewLifecycleOwner) { state ->
            phoneAudioInputState = state
        }
    }

    override fun onDestroyView() {
        super.onDestroyView()
        binding = null

    }

    override fun onDestroy() {
        super.onDestroy()
        mediaPlaybackHandler.stop {
            mediaPlayer?.release()
            mediaPlayer = null
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(PhoneAudioInputPlaybackFragment::class.java)

        fun newInstance(audioFileName: String): PhoneAudioInputPlaybackFragment =
            PhoneAudioInputPlaybackFragment().apply {
                arguments = Bundle().apply {
                    putString(AUDIO_FILE_NAME, audioFileName)
                }
            }
    }
}